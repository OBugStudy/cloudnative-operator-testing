import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Set

from checkpoint.store import _save_json
from core.cr_utils import _collapse_free_form_sub_paths, _flatten_cr_spec

logger = logging.getLogger(__name__)

BLACKLIST_THRESHOLD = 10


def _map_to_declared_field(leaf_path: str, declared_set: set) -> str:
    """Return the longest declared ancestor (or self) of leaf_path.

    E.g. with declared_set={'spec.additionalLabels'},
    'spec.additionalLabels.environment' → 'spec.additionalLabels'.
    Falls back to leaf_path if no declared ancestor found.
    """
    if leaf_path in declared_set:
        return leaf_path
    best: Optional[str] = None
    best_len = 0
    for dp in declared_set:
        if leaf_path.startswith(dp + ".") or leaf_path.startswith(dp + "["):
            if len(dp) > best_len:
                best = dp
                best_len = len(dp)
    return best if best is not None else leaf_path


def _update_field_relations_from_diff(
    field_relations: dict,
    diff: dict,
    cr_before: dict,
    cr_after: dict,
    mutation_round: str = "",
    branch_meta_index: Optional[dict] = None,
    blacklisted_vars: Optional[set] = None,
    blacklisted_exprs: Optional[set] = None,
    declared_field_paths: Optional[set] = None,
    free_form_map_paths: Optional[set] = None,
):

    changed_fields = _get_changed_leaf_fields(
        cr_before, cr_after, declared_field_paths, free_form_map_paths
    )
    print(f"  [tracker] changed_fields={changed_fields}")
    if not changed_fields:
        print("no changed field")
        return


    fmt_lookup = _build_variable_fmt_lookup(branch_meta_index)


    valid_records = []
    for rec in diff.get("changed", []):
        cleaned_rec = _process_and_filter_branch_record(
            rec, branch_meta_index, fmt_lookup, blacklisted_vars
        )
        if cleaned_rec:
            valid_records.append(cleaned_rec)

    if not valid_records:
        print("no valid record")
        return


    affected_bis = {rec["branch_index"] for rec in valid_records}
    print("affected_bi:")
    print(affected_bis)
    for fp in changed_fields:
        _merge_into_field_relations(
            field_relations,
            fp,
            valid_records,
            affected_bis,
            mutation_round,
            branch_meta_index,
            blacklisted_exprs,
        )


def _get_changed_leaf_fields(
    before: dict,
    after: dict,
    declared_field_paths: Optional[set],
    free_form_map_paths: Optional[set] = None,
) -> List[str]:
    """对比 CR 前后差异，找出最精确的变更字段路径"""
    before_spec = _flatten_cr_spec(before)
    after_spec = _flatten_cr_spec(after)

    all_changed = [
        fp
        for fp in set(list(before_spec.keys()) + list(after_spec.keys()))
        if before_spec.get(fp) != after_spec.get(fp)
    ]
    if not all_changed:
        return []

    changed_set = set(all_changed)
    leaf_fields = [
        fp
        for fp in all_changed
        if not any(other != fp and other.startswith(fp) for other in changed_set)
    ]

    if free_form_map_paths:
        leaf_fields = _collapse_free_form_sub_paths(leaf_fields, free_form_map_paths)


    if len(leaf_fields) != 1:
        return []

    leaf_path = leaf_fields[0]
    if declared_field_paths:
        print(
            f"  [tracker debug] leaf_path={repr(leaf_path)}, in declared={leaf_path in declared_field_paths}"
        )
        mapped = _map_to_declared_field(leaf_path, declared_field_paths)
        if mapped != leaf_path and leaf_path not in declared_field_paths:
            leaf_path = mapped

    return [leaf_path]


def _build_variable_fmt_lookup(bmi: Optional[dict]) -> Dict[int, Dict[str, str]]:
    """从静态插桩信息构建变量名查找表"""
    lookup = {}
    if not bmi:
        return lookup
    for bi, bm in bmi.items():
        if not isinstance(bm, dict):
            continue
        bi_vars = {}
        for expr in bm.get("Expressions", []) or []:
            eid = str(expr.get("id", ""))
            for var in expr.get("variables", []):
                vid = str(var.get("id", ""))
                bi_vars[f"{eid}/{vid}"] = (
                    var.get("fmt") or var.get("raw") or var.get("Fmt") or ""
                )
        if bi_vars:
            lookup[int(bi)] = bi_vars
    return lookup


def _process_and_filter_branch_record(
    rec: dict, bmi: Optional[dict], fmt_lookup: dict, blacklisted_vars: Optional[set]
) -> Optional[dict]:
    """清洗单条分支记录，补全 fmt，过滤黑名单，判断是否有效"""
    bi = rec["branch_index"]


    vars_diff = {}
    if "entry_diffs" in rec:
        for entry in rec["entry_diffs"]:
            for k, v in entry.get("variables_diff", {}).items():
                if k not in vars_diff:
                    vars_diff[k] = v.copy()
                else:

                    vars_diff[k]["after_value"] = v["after_value"]
    else:
        vars_diff = {k: v.copy() for k, v in rec.get("variables_diff", {}).items()}


    bi_fmt = fmt_lookup.get(int(bi), {})
    for k, v in vars_diff.items():
        if not v.get("variable_fmt"):
            v["variable_fmt"] = bi_fmt.get(k, "")


    valid_vars = {
        k: v
        for k, v in vars_diff.items()
        if (v.get("before_value") is not None or v.get("after_value") is not None)
        and (
            not blacklisted_vars
            or _var_bl_key(bi, v.get("expression_id", ""), v.get("variable_id", ""))
            not in blacklisted_vars
        )
    }

    bool_changed = rec.get("before_value") != rec.get("after_value")

    logger.info(
        f"  [filter] bi={bi} vars_diff_raw={vars_diff} valid_vars={valid_vars} bool_changed={bool_changed}"
    )


    if valid_vars or bool_changed:
        new_rec = rec.copy()
        new_rec["variables_diff"] = (
            valid_vars
        )
        return new_rec

    return None


def _merge_into_field_relations(
    field_relations: dict,
    fp: str,
    valid_records: List[dict],
    affected_bis: Set[int],
    mutation_round: str,
    bmi: Optional[dict],
    blacklisted_exprs: Optional[set],
):
    """将清洗后的结果合并到全局关系字典中"""
    existing = field_relations.get(fp, {})
    old_indices = set(existing.get("branch_indices", []))
    var_mappings = existing.get("variable_mappings", {})
    ev = existing.get("evidence", {})

    for rec in valid_records:
        bi_key = str(rec["branch_index"])


        entry = {
            "mutation": mutation_round,
            "source": "explore",
            "change": "changed",
            "before_value": rec.get("before_value"),
            "after_value": rec.get("after_value"),
            "variables_diff": rec["variables_diff"],
        }
        ev.setdefault(bi_key, []).append(entry)


        bi_vm = var_mappings.setdefault(bi_key, {})
        for vkey, vinfo in rec["variables_diff"].items():
            vm_entry = bi_vm.setdefault(
                vkey,
                {
                    "variable_fmt": vinfo.get("variable_fmt", ""),
                    "variable_kind": vinfo.get("variable_kind", ""),
                    "evidence": [],
                },
            )
            vm_entry["evidence"].append(
                {
                    "mutation": mutation_round,
                    "before_value": vinfo.get("before_value"),
                    "after_value": vinfo.get("after_value"),
                }
            )


    expr_fmts = existing.get("expression_fmts", {})
    if bmi:
        for bi in affected_bis:
            bi_key = str(bi)
            if bi_key in var_mappings:
                continue

            bm = bmi.get(bi, {})
            no_var_exprs = [
                expr.get("fmt") or expr.get("raw", "")
                for expr in bm.get("Expressions", [])
                if not expr.get("variables") and (expr.get("fmt") or expr.get("raw"))
            ]
            if blacklisted_exprs:
                no_var_exprs = [
                    f for f in no_var_exprs if f"{bi_key}/{f}" not in blacklisted_exprs
                ]

            if no_var_exprs:
                s = set(expr_fmts.get(bi_key, []))
                s.update(no_var_exprs)
                expr_fmts[bi_key] = sorted(s)


    merged_indices = sorted(old_indices | {int(bi) for bi in affected_bis})
    field_relations[fp] = {
        "field_type": existing.get("field_type", ""),
        "branch_indices": merged_indices,
        "total_branches": len(merged_indices),
        "variable_mappings": var_mappings,
        "expression_fmts": expr_fmts,
        "evidence": ev,
        "last_updated": datetime.now().isoformat(),
        "run_id": mutation_round,
    }


def _var_bl_key(bi, expr_id: str, var_id: str) -> str:
    """Build the canonical blacklist key for a variable: 'bi/eid/vid'."""
    return f"{bi}/{expr_id}/{var_id}"


def _expr_bl_key(bi, expr_id: str) -> str:
    """Build the canonical blacklist key for a no-variable expression: 'bi/eid'."""
    return f"{bi}/{expr_id}"


def _build_var_frequency_map(field_relations: dict):
    """Return (var_freq, expr_freq) where each maps bl_key → set of field_paths.

    var_freq  keys: 'bi/eid/vid'  (variable-level blacklist keys)
    expr_freq keys: 'bi/eid'      (expression-level blacklist keys for no-var exprs)
    """
    var_freq: Dict[str, set] = {}
    expr_freq: Dict[str, set] = {}
    for fp, fdata in field_relations.items():
        for bi_key, bi_vm in fdata.get("variable_mappings", {}).items():
            for vkey, vinfo in bi_vm.items():
                if isinstance(vinfo, dict):

                    parts = vkey.split("/", 1)
                    eid = parts[0] if parts else ""
                    vid = parts[1] if len(parts) > 1 else ""
                    key = _var_bl_key(bi_key, eid, vid)
                    var_freq.setdefault(key, set()).add(fp)
        for bi_key, fmts_list in fdata.get("expression_fmts", {}).items():
            for fmt in fmts_list or []:


                key = f"{bi_key}/{fmt}"
                expr_freq.setdefault(key, set()).add(fp)
    return var_freq, expr_freq


def _purge_blacklisted_from_relations(
    field_relations: dict,
    blacklisted_vars: set,
    blacklisted_exprs: set,
) -> None:
    """Remove blacklisted variable mappings and expression fmts from field_relations in-place.

    blacklisted_vars:  set of 'bi/eid/vid' strings
    blacklisted_exprs: set of 'bi/eid/fmt' strings (legacy: 'bi_key/fmt')
    """
    for fdata in field_relations.values():
        vm = fdata.get("variable_mappings", {})
        for bi_key in list(vm.keys()):
            bi_vm = vm[bi_key]
            for vkey in list(bi_vm.keys()):
                vinfo = bi_vm[vkey]
                if not isinstance(vinfo, dict):
                    continue
                parts = vkey.split("/", 1)
                eid = parts[0] if parts else ""
                vid = parts[1] if len(parts) > 1 else ""
                if _var_bl_key(bi_key, eid, vid) in blacklisted_vars:
                    del bi_vm[vkey]
            if not bi_vm:
                del vm[bi_key]

        ef = fdata.get("expression_fmts", {})
        for bi_key in list(ef.keys()):
            ef[bi_key] = [
                fmt
                for fmt in (ef[bi_key] or [])
                if f"{bi_key}/{fmt}" not in blacklisted_exprs
            ]
            if not ef[bi_key]:
                del ef[bi_key]


def _check_and_update_blacklist(
    field_relations: dict,
    config_path: str,
    current_bl_vars: set,
    current_bl_exprs: set,
    threshold: int = BLACKLIST_THRESHOLD,
):
    """Scan field_relations for variables/expressions that appear in >threshold fields.

    Newly discovered items are:
      - Logged with the count of fields they pollute
      - Added to var_blacklist.json (persisted)
      - Purged from field_relations in-place

    Returns the updated (blacklisted_vars, blacklisted_exprs) sets.
    """
    if not config_path:
        return current_bl_vars, current_bl_exprs

    var_freq, expr_freq = _build_var_frequency_map(field_relations)

    newly_bl_vars = {
        key
        for key, fps in var_freq.items()
        if len(fps) > threshold and key not in current_bl_vars
    }
    newly_bl_exprs = {
        key
        for key, fps in expr_freq.items()
        if len(fps) > threshold and key not in current_bl_exprs
    }

    if not newly_bl_vars and not newly_bl_exprs:
        return current_bl_vars, current_bl_exprs

    for key in sorted(newly_bl_vars):
        logger.warning(
            f"[blacklist] 变量 '{key}' 出现在 {len(var_freq[key])} 个字段中"
            f"（阈值={threshold}），加入黑名单并清除已有记录"
        )
    for key in sorted(newly_bl_exprs):
        logger.warning(
            f"[blacklist] 表达式 '{key}' 出现在 {len(expr_freq[key])} 个字段中"
            f"（阈值={threshold}），加入黑名单并清除已有记录"
        )

    all_bl_vars = current_bl_vars | newly_bl_vars
    all_bl_exprs = current_bl_exprs | newly_bl_exprs

    _purge_blacklisted_from_relations(field_relations, newly_bl_vars, newly_bl_exprs)

    _save_var_blacklist(
        config_path,
        {
            "variables": sorted(all_bl_vars),
            "expressions": sorted(all_bl_exprs),
        },
    )
    logger.info(f"[blacklist] 已保存: {_var_blacklist_path(config_path)}")

    return all_bl_vars, all_bl_exprs


def _var_blacklist_path(config_path: str) -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(config_path)), "var_blacklist.json"
    )


def _load_var_blacklist(config_path: str) -> dict:
    """Load var_blacklist.json.

    Returns {"variables": [...], "expressions": [...]} where each list contains
    'bi/eid/vid' or 'bi/eid/fmt' strings (unique composite keys).
    Legacy files that stored plain fmt strings are still loaded as-is; they will
    simply never match the new composite keys and are thus harmlessly ignored.
    """
    path = _var_blacklist_path(config_path)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as _f:
                data = json.load(_f) or {}
            return {
                "variables": data.get("variables", []),
                "expressions": data.get("expressions", []),
            }
        except Exception:
            pass
    return {"variables": [], "expressions": []}


def _save_var_blacklist(config_path: str, data: dict) -> None:
    _save_json(_var_blacklist_path(config_path), data)