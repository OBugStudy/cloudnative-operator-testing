import difflib
import json
import os
from typing import Any, Dict, List, Optional


def diff_branch_sequences(
    before: Optional[Dict[str, Any]],
    after: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """计算两次插桩数据的差值（基于新 traces/expressions/variables 结构）。

    Args:
        before: 前一次 fetch 返回的 InstrumentInfo dict（可为 None）
        after:  后一次 fetch 返回的 InstrumentInfo dict（可为 None）

    Returns:
        diff dict:
        {
            "added":   [branch_index, ...],   # after 有但 before 无
            "removed": [branch_index, ...],   # before 有但 after 无
            "changed": [                       # 两者都有但 value/variables 不同
                {
                    "branch_index": int,
                    "before_value": bool/None,
                    "after_value":  bool/None,
                    "variables_diff": {
                        "<expr_id>/<var_id>": {
                            "expression_id": str,
                            "variable_id": str,
                            "before_value": str,
                            "after_value": str,
                            "variable_kind": str,
                            "variable_fmt": str,
                        }
                    }
                },
                ...
            ],
            "unchanged": [branch_index, ...],
        }

    Update 260403
    计算两次插桩数据的差值（支持同一 branch_index 多条 trace）。

    同一 branch_index 可能携带多条 trace，before/after 各自聚合为列表后：
    - 列表长度或任意对应条目的 value/variables 不同 → changed
    - 完全一致 → unchanged
    """

    def _traces_to_map(data) -> Dict[int, List[Dict]]:
        """将 traces 列表聚合为 {branch_index: [item, ...]}，保留所有条目。"""
        result: Dict[int, List[Dict]] = {}
        if data is None:
            return result
        for item in data.get("traces", []):
            bi = item["branch_index"]
            result.setdefault(bi, []).append(item)
        return result

    def _coerce_key(d, k):
        """Try both str and int key."""
        if k in d:
            return d[k]
        alt = int(k) if isinstance(k, str) else str(k)
        return d.get(alt)

    def _vars_diff_between(b_item: Dict, a_item: Dict) -> Dict[str, Any]:
        """计算两条 trace 之间的 variables 差异。"""
        vars_diff = {}
        b_exprs = b_item.get("expressions", {})
        a_exprs = a_item.get("expressions", {})
        all_eids = set(str(k) for k in b_exprs.keys()) | set(
            str(k) for k in a_exprs.keys()
        )
        for eid in all_eids:
            b_expr = _coerce_key(b_exprs, eid) or {}
            a_expr = _coerce_key(a_exprs, eid) or {}
            b_vars = b_expr.get("variables", {})
            a_vars = a_expr.get("variables", {})
            all_vids = set(str(k) for k in b_vars.keys()) | set(
                str(k) for k in a_vars.keys()
            )
            for vid in all_vids:
                b_var = _coerce_key(b_vars, vid) or {}
                a_var = _coerce_key(a_vars, vid) or {}
                b_val = b_var.get("value")
                a_val = a_var.get("value")
                if b_val != a_val:
                    key = f"{eid}/{vid}"
                    meta = b_var if b_var else a_var
                    vars_diff[key] = {
                        "expression_id": eid,
                        "variable_id": vid,
                        "before_value": b_val,
                        "after_value": a_val,
                        "variable_kind": meta.get("kind", ""),
                        "variable_fmt": meta.get("fmt", "") or meta.get("raw", ""),
                    }
        return vars_diff

    before_map = _traces_to_map(before)
    after_map = _traces_to_map(after)

    before_keys = set(before_map.keys())
    after_keys = set(after_map.keys())

    added = sorted(after_keys - before_keys)
    removed = sorted(before_keys - after_keys)
    changed = []
    unchanged = []

    for bi in sorted(before_keys & after_keys):
        b_items = before_map[bi]
        a_items = after_map[bi]


        max_len = max(len(b_items), len(a_items))
        entry_diffs = []

        for idx in range(max_len):
            b_item = b_items[idx] if idx < len(b_items) else {}
            a_item = a_items[idx] if idx < len(a_items) else {}

            bool_changed = b_item.get("value") != a_item.get("value")
            vars_diff = _vars_diff_between(b_item, a_item)

            if bool_changed or vars_diff:
                entry_diffs.append(
                    {
                        "trace_index": idx,
                        "before_value": b_item.get("value"),
                        "after_value": a_item.get("value"),
                        "variables_diff": vars_diff,
                    }
                )

        if entry_diffs:
            changed.append(
                {
                    "branch_index": bi,

                    "before_value": b_items[0].get("value") if b_items else None,
                    "after_value": a_items[0].get("value") if a_items else None,

                    "trace_count": {"before": len(b_items), "after": len(a_items)},
                    "entry_diffs": entry_diffs,
                }
            )
        else:
            unchanged.append(bi)

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": unchanged,
    }


def _validate_branch_values_from_instr(
    instr_data: Optional[dict], branch_index: int, target_value: str
):
    if instr_data is None:
        return False

    for trace in instr_data.get("traces", []):
        bi = trace.get("branch_index")
        val = trace.get("value")
        if bi == branch_index and val == target_value:
            return True
    return False


def _extract_branch_values_from_instr(
    instr_data: Optional[Dict],
) -> Dict[int, Optional[bool]]:
    """从 instr_data (traces) 中提取 {branch_index: True/False/None}。"""
    if instr_data is None:
        return {}
    result: Dict[int, Optional[bool]] = {}
    for trace in instr_data.get("traces", []):
        bi = trace.get("branch_index")


        val = trace.get("value")
        if val is True or str(val).lower() in ("true", "1", "t"):
            result[bi] = True
        elif val is False or str(val).lower() in ("false", "0", "f"):
            result[bi] = False
        else:
            result[bi] = None
    return result


def _build_branch_index(instrument_info_path: str) -> dict:
    """从 instrument_info_new.json 构建 BranchIndex → branch_point_meta 字典。"""
    if not instrument_info_path or not os.path.exists(instrument_info_path):
        return {}
    try:
        with open(instrument_info_path, "r", encoding="utf-8") as f:
            info = json.load(f)
        return {bp["BranchIndex"]: bp for bp in info.get("branch_points", [])}
    except Exception:
        return {}


def _bm_entry(bm: dict, bi: int) -> dict:
    """Return branch-meta entry for *bi*, trying both int and str keys."""
    return bm.get(bi) or bm.get(str(bi)) or {}


def format_diff_rows(diff: dict, bm: dict) -> List[dict]:
    """Convert a ``diff_branch_sequences`` result into display-ready row dicts.

    Each row has keys: bi, kind, before, after, fmt, file, line, vars.

    Args:
        diff: Output of :func:`diff_branch_sequences`.
        bm:   Branch-meta index from :func:`_build_branch_index`.

    Returns:
        List of row dicts suitable for JSON serialisation.
    """
    rows: List[dict] = []

    for item in diff.get("changed", []):
        if isinstance(item, dict):
            bi, bdata = item.get("branch_index", 0), item
        else:
            bi, bdata = int(item), {}
        entry = _bm_entry(bm, bi)


        if "entry_diffs" in bdata:
            vars_diff: dict = {}
            for entry in bdata["entry_diffs"]:
                for k, v in entry.get("variables_diff", {}).items():
                    prev = vars_diff.get(k)
                    if prev is None:
                        vars_diff[k] = v
                    else:


                        vars_diff[k] = {
                            **v,
                            "before_value": prev["before_value"],
                            "after_value": v["after_value"],
                        }
        else:
            vars_diff = bdata.get("variables_diff", {})


        vars_ = [
            {
                "key": k,
                "fmt": v.get("variable_fmt", ""),
                "before": str(v.get("before_value", "")),
                "after": str(v.get("after_value", "")),
                "kind": v.get("variable_kind", ""),
            }
            for k, v in vars_diff.items()
        ]
        rows.append(
            {
                "bi": bi,
                "kind": "changed",
                "before": str(bdata.get("before_value", "?")),
                "after": str(bdata.get("after_value", "?")),
                "fmt": entry.get("Fmt") or entry.get("Raw", ""),
                "file": entry.get("File") or entry.get("FilePath", ""),
                "line": entry.get("Line") or entry.get("BranchLine", ""),
                "vars": vars_,
            }
        )

    for item in diff.get("added", []):
        bi = item.get("branch_index", 0) if isinstance(item, dict) else int(item)
        entry = _bm_entry(bm, bi)
        rows.append(
            {
                "bi": bi,
                "kind": "added",
                "before": "—",
                "after": "new",
                "fmt": entry.get("Fmt") or entry.get("Raw", ""),
                "file": entry.get("File") or entry.get("FilePath", ""),
                "line": entry.get("Line") or entry.get("BranchLine", ""),
                "vars": [],
            }
        )

    for item in diff.get("removed", []):
        bi = item.get("branch_index", 0) if isinstance(item, dict) else int(item)
        entry = _bm_entry(bm, bi)
        rows.append(
            {
                "bi": bi,
                "kind": "removed",
                "before": "present",
                "after": "—",
                "fmt": entry.get("Fmt") or entry.get("Raw", ""),
                "file": entry.get("File") or entry.get("FilePath", ""),
                "line": entry.get("Line") or entry.get("BranchLine", ""),
                "vars": [],
            }
        )

    return rows


def new_relations_detail(fr_before: dict, fr_after: dict, bm: dict) -> dict:
    """Compute newly established branch relations for a single field.

    Args:
        fr_before: Field-relation dict *before* the apply (copy).
        fr_after:  Field-relation dict *after* the apply (copy).
        bm:        Branch-meta index from :func:`_build_branch_index`.

    Returns:
        Dict with keys: new_count, new_branches, total_before, total_after,
        branch_details (list of enriched branch info dicts).
    """
    before_set = set(fr_before.get("branch_indices", []))
    after_set = set(fr_after.get("branch_indices", []))
    new_bis = after_set - before_set
    details = []
    for bi in sorted(new_bis):
        entry = _bm_entry(bm, bi)
        vm = fr_after.get("variable_mappings", {}).get(str(bi), {})
        details.append(
            {
                "bi": bi,
                "fmt": entry.get("Fmt") or entry.get("Raw", ""),
                "file": entry.get("File") or entry.get("FilePath", ""),
                "line": entry.get("Line") or entry.get("BranchLine", ""),
                "var_count": len(vm),
                "variables": [
                    {
                        "key": k,
                        "fmt": v.get("variable_fmt", ""),
                        "before": str(v.get("before_value", "")),
                        "after": str(v.get("after_value", "")),
                    }
                    for k, v in list(vm.items())[:6]
                ],
            }
        )
    return {
        "new_count": len(new_bis),
        "new_branches": sorted(new_bis),
        "total_before": len(before_set),
        "total_after": len(after_set),
        "branch_details": details,
    }


def cr_diff_html(base_yaml: str, new_yaml: str) -> str:
    """Return an HTML string showing a coloured unified diff of two CR YAMLs.

    Suitable for embedding directly in debugger UI pages.
    """
    base_lines = base_yaml.splitlines(keepends=True)
    new_lines = new_yaml.splitlines(keepends=True)
    diff = list(
        difflib.unified_diff(
            base_lines, new_lines, fromfile="base_cr.yaml", tofile="mutated_cr.yaml"
        )
    )
    if not diff:
        return "<em style='color:#8892a0'>（CR 无变化）</em>"
    parts = []
    for line in diff:
        line = line.rstrip("\n")
        esc = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        if line.startswith("+++") or line.startswith("---"):
            parts.append(f'<span style="color:#8892a0">{esc}</span>')
        elif line.startswith("@@"):
            parts.append(f'<span style="color:#a855f7">{esc}</span>')
        elif line.startswith("+"):
            parts.append(f'<span style="color:#22c55e">{esc}</span>')
        elif line.startswith("-"):
            parts.append(f'<span style="color:#ef4444">{esc}</span>')
        else:
            parts.append(f'<span style="color:#9ca3af">{esc}</span>')
    return "<br>".join(parts)