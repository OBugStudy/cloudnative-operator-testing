

import logging
import time as _time
from typing import Dict, List, Optional

import yaml

from cluster.apply import apply_cr_and_collect
from core.cr_utils import (
    _FIELD_MISSING,
    _cr_changed_fields,
    _field_exists_in_cr,
    _get_current_field_value,
)
from instrumentation.diff import diff_branch_sequences
from phases.explore_all import (
    _check_trace_health,
    _execute_single_mutation,
)
from relations.tracker import (
    _check_and_update_blacklist,
    _load_var_blacklist,
    _purge_blacklisted_from_relations,
    _update_field_relations_from_diff,
)

logger = logging.getLogger(__name__)


def _load_cr_from_yaml(yaml_str: str, namespace: str, cr_kind: str) -> Optional[dict]:
    """Parse a CR YAML string; patch namespace/kind if missing."""
    if not yaml_str:
        return None
    try:
        cr = yaml.safe_load(yaml_str)
    except Exception as exc:
        logger.warning(f"YAML 解析失败: {exc}")
        return None
    if not isinstance(cr, dict) or "spec" not in cr:
        return None
    cr.setdefault("metadata", {}).setdefault("namespace", namespace)
    if not cr.get("kind"):
        cr["kind"] = cr_kind
    return cr


def _fmt_val(v, maxlen: int = 60) -> str:
    if v is _FIELD_MISSING:
        return "(absent)"
    return repr(v)[:maxlen]


def _aggregate_variables_diff(brec: dict) -> dict:
    """Aggregate variables_diff from entry_diffs (new format) or top-level (old format)."""
    if "entry_diffs" in brec:
        vd: dict = {}
        for ed in brec["entry_diffs"]:
            for k, v in ed.get("variables_diff", {}).items():
                prev = vd.get(k)
                if prev is None:
                    vd[k] = v
                else:
                    vd[k] = {**v, "before_value": prev["before_value"]}
        return vd
    return brec.get("variables_diff", {})


def _format_branch_lines(diff: dict, recorded_bis: set) -> List[str]:
    lines: List[str] = []
    for brec in diff.get("changed", []):
        bi = brec.get("branch_index", "?")
        bval = brec.get("before_value")
        aval = brec.get("after_value")
        bool_part = f"  取值: {bval} → {aval}" if bval != aval else ""
        vd = _aggregate_variables_diff(brec)
        var_parts: List[str] = []
        for v in vd.values():
            fmt = v.get("variable_fmt") or v.get("variable_id", "?")
            bv2 = repr(v.get("before_value"))[:40]
            av2 = repr(v.get("after_value"))[:40]
            var_parts.append(f"{fmt}: {bv2} → {av2}")
        var_str = "  |  ".join(var_parts[:4])
        if len(var_parts) > 4:
            var_str += f"  … (+{len(var_parts) - 4})"
        mark = "✓" if bi in recorded_bis else "○"
        lines.append(
            f"    [{mark}] b[{bi}]{bool_part}"
            + (f"  变量: {var_str}" if var_str else "  (无变量 diff)")
        )
    for bi in diff.get("added", []):
        lines.append(f"    [新增] b[{bi}]")
    return lines


def _rerun_field_exploration(
    field_path: str,
    healthy_baseline_cr: dict,
    healthy_baseline_instr: dict,
    namespace: str,
    cr_kind: str,
    kubectl_client,
    cluster_name: str,
    operator_container_name: str,
    wait_sec: int,
    collect_max_wait: int,
    field_relations: dict,
    branch_meta_index: Optional[dict],
    config_path: str,
    blacklisted_vars: set,
    blacklisted_exprs: set,
    declared_field_paths: Optional[set],
    instrument_prefix: str,
    all_required_fields: List[str],
    crd_file: str,
    seed_cr: dict,
    max_retries: int = 3,
) -> dict:
    """Re-run a single-field LLM mutation from the known-good base CR.

    Called when validate detected an unhealthy trace for a field replay.
    Returns a correction dict with keys:
      corrected, new_result, error
    """
    logger.info(f"    [correction] 从健康基准 CR 重新探测字段 {field_path}...")
    field_present = _field_exists_in_cr(healthy_baseline_cr, field_path)
    try:
        sr = _execute_single_mutation(
            field_path=field_path,
            field_present=field_present,
            seed_cr=seed_cr,
            namespace=namespace,
            base_cr=healthy_baseline_cr,
            base_instr=healthy_baseline_instr,
            field_base_cr_yaml=yaml.dump(healthy_baseline_cr),
            all_required_fields=all_required_fields,
            crd_file=crd_file,
            cr_kind=cr_kind,
            kubectl_client=kubectl_client,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            field_relations=field_relations,
            branch_meta_index=branch_meta_index,
            config_path=config_path,
            blacklisted_vars=blacklisted_vars,
            blacklisted_exprs=blacklisted_exprs,
            max_retries=max_retries,
            instrument_prefix=instrument_prefix,
            declared_field_paths=declared_field_paths,
        )
    except Exception as exc:
        logger.error(f"    [correction] 重新探测异常: {exc}")
        return {"corrected": False, "new_result": None, "error": str(exc)}

    if not sr.get("success"):
        logger.warning(f"    [correction] 重新探测失败: {sr.get('error', '')}")
        return {"corrected": False, "new_result": None, "error": sr.get("error", "")}

    diff = sr.get("diff", {})
    after_instr = sr.get("after_instr")
    mutated_cr = sr.get("mutated_cr")
    n_c = len(diff.get("changed", []))
    n_a = len(diff.get("added", []))
    n_r = len(diff.get("removed", []))
    fr_now = field_relations.get(field_path, {})
    new_bis = sorted(set(fr_now.get("branch_indices", [])))
    logger.info(
        f"    [correction] 重新探测成功: changed={n_c} added={n_a} removed={n_r}  "
        f"新增关联 branch: {new_bis}"
    )
    new_result = {
        "field": field_path,
        "sub_kind": "correction",
        "replay_status": "ok",
        "replay_diff": {"changed": n_c, "added": n_a, "removed": n_r},
        "diff_raw": diff,
        "recorded_branch_indices": new_bis,
        "before_instr": healthy_baseline_instr,
        "after_instr": after_instr,
        "base_cr_yaml": yaml.dump(healthy_baseline_cr),
        "mutated_cr_yaml": sr.get("mutated_cr_yaml", ""),
        "cr_changed_fields": _cr_changed_fields(healthy_baseline_cr, mutated_cr)
        if mutated_cr
        else [],
        "branch_lines": _format_branch_lines(diff, set(new_bis)),
        "skip_reason": "",
        "error": "",
    }
    return {"corrected": True, "new_result": new_result, "error": ""}


def _replay_one_field(
    field_path: str,
    sub: dict,
    baseline_instr: dict,
    namespace: str,
    cr_kind: str,
    kubectl_client,
    cluster_name: str,
    operator_container_name: str,
    wait_sec: int,
    collect_max_wait: int,
    field_relations: dict,
    branch_meta_index: Optional[dict],
    config_path: str,
    blacklisted_vars: set,
    blacklisted_exprs: set,
    declared_field_paths: Optional[set],
    instrument_prefix: str,
    dry_run: bool,
    healthy_baseline_instr: Optional[dict] = None,
    healthy_baseline_cr: Optional[dict] = None,
    seed_cr: Optional[dict] = None,
    all_required_fields: Optional[List[str]] = None,
    crd_file: str = "",
    max_retries: int = 3,
) -> dict:
    """Replay a single sub-mutation entry.  Returns a result dict."""
    base_cr_yaml: str = sub.get("base_cr_yaml", "")
    mutated_cr_yaml: str = sub.get("mutated_cr_yaml", "")

    result: dict = {
        "field": field_path,
        "sub_kind": sub.get("kind", "?"),
        "original_status": sub.get("status", "?"),
        "original_diff": sub.get("diff_summary", {}),
        "replay_status": "skip",
        "replay_diff": {},
        "branch_lines": [],
        "recorded_branch_indices": [],
        "skip_reason": "",
        "error": "",

        "base_cr_yaml": base_cr_yaml,
        "mutated_cr_yaml": mutated_cr_yaml,
        "before_instr": None,
        "after_instr": None,
        "diff_raw": {},
    }

    if not base_cr_yaml:
        result["skip_reason"] = "base_cr_yaml 缺失"
        return result
    if not mutated_cr_yaml:
        result["skip_reason"] = "mutated_cr_yaml 缺失"
        return result

    base_cr = _load_cr_from_yaml(base_cr_yaml, namespace, cr_kind)
    mutated_cr = _load_cr_from_yaml(mutated_cr_yaml, namespace, cr_kind)
    if base_cr is None:
        result["skip_reason"] = "base_cr YAML 解析失败"
        return result
    if mutated_cr is None:
        result["skip_reason"] = "mutated_cr YAML 解析失败"
        return result


    bv = _get_current_field_value(base_cr, field_path)
    av = _get_current_field_value(mutated_cr, field_path)
    result["field_before"] = None if bv is _FIELD_MISSING else repr(bv)[:120]
    result["field_after"] = None if av is _FIELD_MISSING else repr(av)[:120]

    logger.info(f"  字段: {field_path}  ({sub.get('kind', '?')})")
    logger.info(f"    值: {_fmt_val(bv)}  →  {_fmt_val(av)}")

    if dry_run:
        result["replay_status"] = "dry-run"
        result["skip_reason"] = "dry-run 模式，跳过实际 apply"
        logger.info("    [dry-run] 跳过 apply")
        result["base_cr_yaml"] = base_cr_yaml
        result["mutated_cr_yaml"] = mutated_cr_yaml
        return result


    logger.info("    [1/2] apply base_cr → 收集 before_instr...")
    t0 = _time.monotonic()
    before_instr, _, ok_b, _, cluster_dead = apply_cr_and_collect(
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        input_cr=base_cr,
        operator_container_name=operator_container_name,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        instrument_prefix=instrument_prefix,
    )
    if cluster_dead:
        result["replay_status"] = "error"
        result["error"] = "集群控制器 Pod 无法恢复"
        logger.error("    集群 Pod 无法恢复，中止本字段验证")
        return result
    if not ok_b or before_instr is None:
        result["replay_status"] = "error"
        result["error"] = "base_cr apply/collect 失败"
        logger.warning("    base_cr apply/collect 失败")
        return result
    result["before_instr"] = before_instr
    logger.info(f"    before_instr 收集完成  {_time.monotonic() - t0:.1f}s")


    logger.info("    [2/2] apply mutated_cr → 收集 after_instr...")
    t1 = _time.monotonic()
    after_instr, _, ok_a, _, cluster_dead = apply_cr_and_collect(
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        input_cr=mutated_cr,
        operator_container_name=operator_container_name,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        instrument_prefix=instrument_prefix,
    )
    if cluster_dead:
        result["replay_status"] = "error"
        result["error"] = "集群控制器 Pod 无法恢复（mutated CR apply）"
        logger.error("    集群 Pod 无法恢复，中止本字段验证")
        return result
    if not ok_a or after_instr is None:
        result["replay_status"] = "error"
        result["error"] = "mutated_cr apply/collect 失败"
        logger.warning("    mutated_cr apply/collect 失败")
        return result
    result["after_instr"] = after_instr
    logger.info(f"    after_instr  收集完成  {_time.monotonic() - t1:.1f}s")


    diff = diff_branch_sequences(before_instr, after_instr)
    result["diff_raw"] = diff
    n_c = len(diff.get("changed", []))
    n_a = len(diff.get("added", []))
    n_r = len(diff.get("removed", []))
    result["replay_diff"] = {"changed": n_c, "added": n_a, "removed": n_r}


    _update_field_relations_from_diff(
        field_relations=field_relations,
        diff=diff,
        cr_before=base_cr,
        cr_after=mutated_cr,
        mutation_round=f"validate-{field_path}",
        branch_meta_index=branch_meta_index,
        blacklisted_vars=blacklisted_vars,
        blacklisted_exprs=blacklisted_exprs,
        declared_field_paths=declared_field_paths,
    )
    blacklisted_vars, blacklisted_exprs = _check_and_update_blacklist(
        field_relations, config_path, blacklisted_vars, blacklisted_exprs
    )


    fr_now = field_relations.get(field_path, {})
    recorded_bis: set = set(fr_now.get("branch_indices", []))
    result["recorded_branch_indices"] = sorted(recorded_bis)


    cr_changed = _cr_changed_fields(base_cr, mutated_cr)
    result["cr_changed_fields"] = cr_changed
    n_leaf = len(cr_changed)
    tracker_skipped = n_leaf != 1


    branch_lines = _format_branch_lines(diff, recorded_bis)
    result["branch_lines"] = branch_lines


    fp_disp = ", ".join(cr_changed[:5])
    if len(cr_changed) > 5:
        fp_disp += f" … (+{len(cr_changed) - 5})"
    logger.info(
        f"    分支 diff: changed={n_c}  added={n_a}  removed={n_r}\n"
        f"    CR 变更字段: [{fp_disp}] ({n_leaf} 个)"
    )
    for line in branch_lines:
        logger.info(line)

    if tracker_skipped:
        skip_msg = (
            f"    ⚠ 关联未记录: CR 同时变化了 {n_leaf} 个叶子字段"
            f"（需精确变更 1 个字段才可建立关联）"
        )
        result["skip_reason"] = f"multi-leaf ({n_leaf} fields changed simultaneously)"
        logger.info(skip_msg)
    elif not recorded_bis:
        result["skip_reason"] = "diff 为空，无可记录的关联"
        logger.info("    ℹ 无有效关联可记录（diff 为空或所有变量被过滤）")

    n_rel = len(recorded_bis)
    logger.info(f"    累计关联分支: {n_rel} 个  (字段 {field_path})")


    healthy_trace_count = len((healthy_baseline_instr or {}).get("traces", []))
    _after_healthy = _check_trace_health(after_instr, healthy_trace_count)
    result["trace_healthy"] = _after_healthy

    if not _after_healthy and healthy_baseline_cr is not None and not dry_run:
        logger.warning(
            f"    [health] after_instr trace 数量异常（基准={healthy_trace_count}），"
            "触发修正重探测..."
        )
        result["correction_triggered"] = True
        correction = _rerun_field_exploration(
            field_path=field_path,
            healthy_baseline_cr=healthy_baseline_cr,
            healthy_baseline_instr=healthy_baseline_instr,
            namespace=namespace,
            cr_kind=cr_kind,
            kubectl_client=kubectl_client,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            field_relations=field_relations,
            branch_meta_index=branch_meta_index,
            config_path=config_path,
            blacklisted_vars=blacklisted_vars,
            blacklisted_exprs=blacklisted_exprs,
            declared_field_paths=declared_field_paths,
            instrument_prefix=instrument_prefix,
            all_required_fields=all_required_fields or [],
            crd_file=crd_file,
            seed_cr=seed_cr or healthy_baseline_cr,
            max_retries=max_retries,
        )
        result["correction"] = correction
        if correction.get("corrected") and correction.get("new_result"):
            nr = correction["new_result"]
            result["replay_status"] = "ok_corrected"
            result["replay_diff"] = nr["replay_diff"]
            result["diff_raw"] = nr["diff_raw"]
            result["recorded_branch_indices"] = nr["recorded_branch_indices"]
            result["after_instr"] = nr["after_instr"]
            result["mutated_cr_yaml"] = nr["mutated_cr_yaml"]
            result["cr_changed_fields"] = nr["cr_changed_fields"]
            result["branch_lines"] = nr["branch_lines"]
        else:
            result["replay_status"] = "ok_unhealthy"
    else:
        result["correction_triggered"] = False
        result["correction"] = None
        result["trace_healthy"] = _after_healthy
        result["replay_status"] = "ok"
    return result


def run_field_validation(
    ckpt: dict,
    field_paths: List[str],
    namespace: str,
    cr_kind: str,
    kubectl_client,
    cluster_name: str,
    operator_container_name: str,
    wait_sec: int,
    collect_max_wait: int,
    branch_meta_index: Optional[dict],
    config_path: str,
    instrument_prefix: str,
    declared_field_paths: Optional[set],
    dry_run: bool = False,
    healthy_baseline_instr: Optional[dict] = None,
    healthy_baseline_cr: Optional[dict] = None,
    seed_cr: Optional[dict] = None,
    all_required_fields: Optional[List[str]] = None,
    crd_file: str = "",
    max_retries: int = 3,
) -> dict:
    """Validate recorded mutations in an explore-all checkpoint.

    For each specified field_path, find the mutation_log entry, re-apply
    base_cr → collect before, mutated_cr → collect after, re-diff, and
    update field_relations.  Returns a report dict.
    """
    logger.info("=" * 70)
    logger.info("GSOD — Validate 模式  (变异重放验证)")
    logger.info("=" * 70)

    ea = ckpt.get("explore_all", {})
    mutation_log: list = ea.get("mutation_log", [])
    field_relations: dict = ckpt.setdefault("field_relations", {})


    ml_index: Dict[str, list] = {}
    for entry in mutation_log:
        fp = entry.get("field", "")
        ml_index.setdefault(fp, []).append(entry)


    if config_path:
        _bl = _load_var_blacklist(config_path)
        blacklisted_vars: set = set(_bl.get("variables", []))
        blacklisted_exprs: set = set(_bl.get("expressions", []))
        if blacklisted_vars or blacklisted_exprs:
            _purge_blacklisted_from_relations(
                field_relations, blacklisted_vars, blacklisted_exprs
            )
    else:
        blacklisted_vars = set()
        blacklisted_exprs = set()

    report: dict = {
        "fields_requested": field_paths,
        "fields_found": [],
        "fields_missing": [],
        "results": [],
    }

    for fp in field_paths:
        entries = ml_index.get(fp)
        if not entries:
            logger.warning(f"[validate] 字段 {fp} 在 mutation_log 中未找到，跳过")
            report["fields_missing"].append(fp)
            continue

        report["fields_found"].append(fp)
        logger.info(f"\n{'─' * 60}")
        logger.info(f"[validate] 验证字段: {fp}  ({len(entries)} 条记录)")

        for entry in entries:
            subs = entry.get("sub_mutations", [])
            if not subs:
                logger.info("  ⚠ sub_mutations 为空，跳过")
                continue

            for sub in subs:
                if sub.get("status") != "ok":
                    logger.info(
                        f"  ↷ 原始状态 {sub.get('status', '?')}，跳过（原始变异失败）"
                    )
                    continue

                res = _replay_one_field(
                    field_path=fp,
                    sub=sub,
                    baseline_instr=ea.get("baseline_instr", {}),
                    namespace=namespace,
                    cr_kind=cr_kind,
                    kubectl_client=kubectl_client,
                    cluster_name=cluster_name,
                    operator_container_name=operator_container_name,
                    wait_sec=wait_sec,
                    collect_max_wait=collect_max_wait,
                    field_relations=field_relations,
                    branch_meta_index=branch_meta_index,
                    config_path=config_path,
                    blacklisted_vars=blacklisted_vars,
                    blacklisted_exprs=blacklisted_exprs,
                    declared_field_paths=declared_field_paths,
                    instrument_prefix=instrument_prefix,
                    healthy_baseline_instr=healthy_baseline_instr,
                    healthy_baseline_cr=healthy_baseline_cr,
                    seed_cr=seed_cr,
                    all_required_fields=all_required_fields,
                    crd_file=crd_file,
                    max_retries=max_retries,
                    dry_run=dry_run,
                )
                report["results"].append(res)


    n_ok = sum(1 for r in report["results"] if r["replay_status"] == "ok")
    n_corrected = sum(
        1 for r in report["results"] if r["replay_status"] == "ok_corrected"
    )
    n_unhealthy = sum(
        1 for r in report["results"] if r["replay_status"] == "ok_unhealthy"
    )
    n_err = sum(1 for r in report["results"] if r["replay_status"] == "error")
    n_skip = sum(
        1 for r in report["results"] if r["replay_status"] in ("skip", "dry-run")
    )
    n_rel_total = sum(
        len(r.get("recorded_branch_indices", [])) for r in report["results"]
    )
    logger.info("=" * 70)
    logger.info(
        f"验证完成: {n_ok} 正常 / {n_corrected} 修正成功 / {n_unhealthy} 修正失败 / "
        f"{n_err} 错误 / {n_skip} 跳过  |  新增关联分支: {n_rel_total} 个"
    )
    logger.info("=" * 70)

    report["summary"] = {
        "ok": n_ok,
        "ok_corrected": n_corrected,
        "ok_unhealthy": n_unhealthy,
        "error": n_err,
        "skip": n_skip,
        "new_branch_relations": n_rel_total,
    }
    return report