

import copy
import json
import logging
import os
import re
import time as _time
from collections import deque
from typing import List, Optional, Set

import yaml

import cluster.apply as _cluster_apply
from acto.kubectl_client import KubectlClient
from checkpoint.store import (
    _save_checkpoint,
    _update_branch_baseline_crs,
)
from cluster.apply import (
    apply_cr_and_collect,
    fetch_operator_error_logs,
    get_operator_log_line_count,
)
from core.cr_utils import (
    _FIELD_MISSING,
    _collapse_free_form_sub_paths,
    _cr_changed_fields,
    _field_exists_in_cr,
    _fmt_current_value,
    _get_current_field_value,
)
from core.patch import _apply_patch_to_cr, _delete_field_from_cr, _parse_llm_patch
from core.rich_logger import update_progress, update_status
from core.timing import _timed_step
from crd.schema import (
    _extract_crd_required_fields,
    _extract_free_form_map_paths,
    _extract_required_siblings,
)
from crd.validation import _repair_required_fields, _validate_patch_against_crd
from instrumentation.diff import diff_branch_sequences
from llm.client import _call_llm_for_branch_flip
from llm.prompts import (
    _build_direct_value_prompt,
    _build_explore_add_prompt,
    _build_phase1_prompt,
)
from relations.tracker import (
    _check_and_update_blacklist,
    _load_var_blacklist,
    _purge_blacklisted_from_relations,
    _update_field_relations_from_diff,
)

logger = logging.getLogger(__name__)


_TRACE_HEALTH_RATIO = 0.4


def _normalize_path_for_matching(path_parts: list) -> str:
    """Normalize acto path list ['spec','racks','ITEM','rack'] -> 'spec.racks.rack'."""
    normalized = []
    for p in path_parts:
        if isinstance(p, int):
            continue
        if isinstance(p, str) and (p.isdigit() or p.upper() == "ITEM"):
            continue
        normalized.append(str(p))
    return ".".join(normalized)


def _normalize_field_path_for_matching(field_path: str) -> str:
    """Normalize 'spec.racks[*].rack' -> 'spec.racks.rack'."""
    return re.sub(r"\[\*?\d*\]", "", field_path).replace("..", ".").strip(".")


def _build_acto_field_map(acto_input_model) -> dict:
    """Return dict: normalized_dot_path -> [(path_str, TestCase), ...]."""
    field_map: dict = {}
    for group in acto_input_model.normal_test_plan_partitioned[0]:
        for path_str, testcase in group:
            try:
                path_list = json.loads(path_str)
            except Exception:
                continue
            key = _normalize_path_for_matching(path_list)
            field_map.setdefault(key, []).append((path_str, testcase))
    return field_map


def _execute_acto_step(
    testcase,
    path_str: str,
    acto_input_model,
    seed_cr: dict,
    namespace: str,
    base_instr: dict,
    field_path: str,
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
    step_idx: int,
    instrument_prefix: str = "",
    declared_field_paths: Optional[set] = None,
    free_form_map_paths: Optional[set] = None,
) -> dict:
    """Apply one acto TestCase mutation without LLM; return same dict as _execute_change_step."""
    from acto.engine import apply_testcase as _apply_testcase
    from acto.input.value_with_schema import attach_schema_to_value as _attach

    def _failure(err, apply_s=0.0):
        return {
            "success": False,
            "error": err,
            "after_instr": None,
            "diff": {},
            "mutated_cr": None,
            "mutated_cr_yaml": "",
            "llm_sec": None,
            "apply_sec": apply_s,
            "blacklisted_vars": blacklisted_vars,
            "blacklisted_exprs": blacklisted_exprs,
        }

    try:
        path_list = json.loads(path_str)
    except Exception as e:
        return _failure(f"path_str parse error: {e}")

    cr_with_schema = _attach(copy.deepcopy(seed_cr), acto_input_model.get_root_schema())
    try:
        _apply_testcase(cr_with_schema, path_list, testcase)
    except Exception as e:
        logger.info(f"  [acto skip] apply_testcase 失败 ({type(e).__name__}): {e}")
        return _failure(f"apply_testcase: {e}")

    mutated_cr = cr_with_schema.raw_value()
    mutated_cr.setdefault("metadata", {})["name"] = seed_cr.get("metadata", {}).get(
        "name", "test-cluster"
    )
    mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
    mutated_cr_yaml = yaml.dump(mutated_cr)

    _t_apply = _time.monotonic()
    after_instr_tmp, _, ok, _, _ = apply_cr_and_collect(
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        input_cr=mutated_cr,
        operator_container_name=operator_container_name,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        instrument_prefix=instrument_prefix,
    )
    apply_sec = round(_time.monotonic() - _t_apply, 2)

    if not ok or after_instr_tmp is None:
        stderr = _cluster_apply._last_create_stderr
        error = stderr.strip() if stderr.strip() else "kubectl create failed (acto)"
        logger.warning(f"  [acto] apply/collect 失败: {error[:200]}")
        return _failure(error, apply_sec)

    sub_diff = diff_branch_sequences(base_instr, after_instr_tmp)
    logger.info(
        f"  ✓ acto diff: changed={len(sub_diff.get('changed', []))}, "
        f"added={len(sub_diff.get('added', []))}, removed={len(sub_diff.get('removed', []))}"
    )
    _update_field_relations_from_diff(
        field_relations=field_relations,
        diff=sub_diff,
        cr_before=seed_cr,
        cr_after=mutated_cr,
        mutation_round=f"acto-{field_path}-step{step_idx + 1}",
        branch_meta_index=branch_meta_index,
        blacklisted_vars=blacklisted_vars,
        blacklisted_exprs=blacklisted_exprs,
        declared_field_paths=declared_field_paths,
        free_form_map_paths=free_form_map_paths,
    )
    blacklisted_vars, blacklisted_exprs = _check_and_update_blacklist(
        field_relations, config_path, blacklisted_vars, blacklisted_exprs
    )
    return {
        "success": True,
        "error": "",
        "after_instr": after_instr_tmp,
        "diff": sub_diff,
        "mutated_cr": mutated_cr,
        "mutated_cr_yaml": mutated_cr_yaml,
        "llm_sec": None,
        "apply_sec": apply_sec,
        "blacklisted_vars": blacklisted_vars,
        "blacklisted_exprs": blacklisted_exprs,
    }


def _collect_exploration_baseline(
    ea: dict,
    ckpt: dict,
    ckpt_path: str,
    seed_cr: dict,
    seed_cr_yaml: str,
    kubectl_client,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    wait_sec: int,
    collect_max_wait: int,
    config_path: str,
    instrument_prefix: str = "",
) -> Optional[dict]:
    """首次收集或从 checkpoint 恢复 seed CR 基准插桩数据。返回 baseline_instr，失败返回 None。"""
    if not ea.get("baseline_collected"):
        logger.info("[explore-all] 收集基准插桩数据...")
        baseline_instr, _, baseline_ok, _, _ = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=seed_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
        )
        if not baseline_ok or baseline_instr is None:
            logger.error("[explore-all] 基准数据收集失败，终止")
            return None
        ea["baseline_instr"] = baseline_instr
        ea["baseline_collected"] = True
        if config_path:
            _update_branch_baseline_crs(config_path, baseline_instr, seed_cr_yaml)
        _save_checkpoint(ckpt_path, ckpt)
    else:
        baseline_instr = ea.get("baseline_instr")
        if baseline_instr is None:
            logger.error(
                "[explore-all] checkpoint 中缺少基准数据，请删除 checkpoint 重新运行"
            )
            return None
        logger.info("[explore-all] 使用 checkpoint 中的基准数据")
    return baseline_instr


def _restore_rolling_baseline(ea: dict, seed_cr: dict) -> tuple:
    """从 checkpoint 恢复滚动基准（current_instr, current_cr）。"""
    if ea.get("current_instr") and ea.get("current_cr_yaml"):
        current_instr = ea["current_instr"]
        try:
            current_cr = yaml.safe_load(ea["current_cr_yaml"])
        except Exception:
            current_cr = seed_cr
        logger.info("[explore-all] 从 checkpoint 恢复滚动基准")
    else:
        current_instr = ea.get("baseline_instr")
        current_cr = seed_cr
    return current_instr, current_cr


def _check_trace_health(after_instr: Optional[dict], healthy_trace_count: int) -> bool:
    """Return True if the instrumentation looks healthy (enough trace entries)."""
    if after_instr is None:
        return False
    n = len(after_instr.get("traces", []))
    if healthy_trace_count <= 0:
        return n > 0
    ratio = n / healthy_trace_count
    if ratio < _TRACE_HEALTH_RATIO:
        logger.warning(
            f"[health] trace 数量异常收缩: {n} / {healthy_trace_count} "
            f"(ratio={ratio:.2f} < {_TRACE_HEALTH_RATIO})，视为无效结果"
        )
        return False
    return True


def _grep_project_for_error(
    error_lines: List[str],
    project_path: str,
    max_results: int = 3,
    context_lines: int = 4,
) -> str:
    if not project_path or not os.path.isdir(project_path):
        return ""
    if not error_lines:
        return ""
    keywords: List[str] = []
    for line in error_lines[:5]:
        for m in re.finditer(r"['\"]([A-Za-z][A-Za-z0-9_\.]{3,})['\"]", line):
            kw = m.group(1)
            if kw not in keywords:
                keywords.append(kw)
        for m in re.finditer(r"\b([A-Z][a-z]+[A-Z][A-Za-z0-9]+)\b", line):
            kw = m.group(1)
            if kw not in keywords:
                keywords.append(kw)
    if not keywords:
        return ""
    snippets: List[str] = []
    seen_files: set = set()
    for kw in keywords[:4]:
        if len(snippets) >= max_results:
            break
        try:
            result = __import__("subprocess").run(
                ["grep", "-rn", "--include=*.go", "-l", kw, project_path],
                capture_output=True,
                text=True,
                timeout=10,
            )
            for fpath in (result.stdout or "").splitlines()[:3]:
                if fpath in seen_files or len(snippets) >= max_results:
                    break
                seen_files.add(fpath)
                grep2 = __import__("subprocess").run(
                    ["grep", "-n", kw, fpath],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for match_line in (grep2.stdout or "").splitlines()[:2]:
                    try:
                        lineno = int(match_line.split(":")[0]) - 1
                    except ValueError:
                        continue
                    with open(fpath, encoding="utf-8", errors="ignore") as fh:
                        all_lines = fh.readlines()
                    start = max(0, lineno - context_lines)
                    end = min(len(all_lines), lineno + context_lines + 1)
                    snippet = "".join(all_lines[start:end])
                    rel = os.path.relpath(fpath, project_path)
                    snippets.append(f"// {rel}:{lineno + 1}\n{snippet}")
                    if len(snippets) >= max_results:
                        break
        except Exception:
            pass
    if not snippets:
        return ""
    return (
        f"[源码上下文（来自项目搜索 '{keywords[0]}'）]\n"
        + "\n---\n".join(snippets)
        + "\n"
    )


def _collect_collateral_fields(
    field_path: str,
    sub_mutated_cr: Optional[dict],
    base_cr: dict,
    sub_diff: dict,
    declared_field_paths: Optional[set],
    completed_set: set,
    free_form_map_paths: Optional[set] = None,
) -> List[str]:
    if sub_mutated_cr is None:
        return []
    if not sub_diff.get("changed") and not sub_diff.get("added"):
        return []
    changed_fps = _cr_changed_fields(base_cr, sub_mutated_cr)
    others: List[str] = []
    seen: set = set()
    for fp in changed_fps:
        if fp == field_path:
            continue
        effective = fp
        if free_form_map_paths:
            for ffm in free_form_map_paths:
                if fp.startswith(ffm + ".") or fp.startswith(ffm + "["):
                    effective = ffm
                    break
        if effective == field_path:
            continue
        if effective in completed_set:
            continue
        if effective not in seen:
            seen.add(effective)
            others.append(effective)
    return others


def _execute_remove_step(
    field_path: str,
    seed_cr: dict,
    namespace: str,
    base_cr: dict,
    base_instr: dict,
    field_base_cr_yaml: str,
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
    instrument_prefix: str = "",
    declared_field_paths: Optional[set] = None,
    free_form_map_paths: Optional[set] = None,
) -> dict:
    """执行 remove 步骤：直接删除字段后 apply，更新关联映射。"""
    _remove_base = yaml.safe_load(field_base_cr_yaml) or base_cr
    mutated_cr = _delete_field_from_cr(_remove_base, field_path)
    mutated_cr.setdefault("metadata", {})["name"] = seed_cr["metadata"]["name"]
    mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
    mutated_cr_yaml = yaml.dump(mutated_cr)

    _t_apply = _time.monotonic()
    after_instr_tmp, _, ok, _, _ = apply_cr_and_collect(
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        input_cr=mutated_cr,
        operator_container_name=operator_container_name,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        instrument_prefix=instrument_prefix,
    )
    apply_sec = round(_time.monotonic() - _t_apply, 2)

    if ok and after_instr_tmp is not None:
        sub_diff = diff_branch_sequences(base_instr, after_instr_tmp)
        logger.info(
            f"  ✓ remove diff: changed={len(sub_diff.get('changed', []))}, "
            f"added={len(sub_diff.get('added', []))}, removed={len(sub_diff.get('removed', []))}"
        )
        _update_field_relations_from_diff(
            field_relations=field_relations,
            diff=sub_diff,
            cr_before=base_cr,
            cr_after=mutated_cr,
            mutation_round=f"explore-all-{field_path}-remove",
            branch_meta_index=branch_meta_index,
            blacklisted_vars=blacklisted_vars,
            blacklisted_exprs=blacklisted_exprs,
            declared_field_paths=declared_field_paths,
            free_form_map_paths=free_form_map_paths,
        )
        blacklisted_vars, blacklisted_exprs = _check_and_update_blacklist(
            field_relations, config_path, blacklisted_vars, blacklisted_exprs
        )
        return {
            "success": True,
            "error": "",
            "after_instr": after_instr_tmp,
            "diff": sub_diff,
            "mutated_cr": mutated_cr,
            "mutated_cr_yaml": mutated_cr_yaml,
            "apply_sec": apply_sec,
            "llm_sec": None,
            "blacklisted_vars": blacklisted_vars,
            "blacklisted_exprs": blacklisted_exprs,
        }
    else:
        stderr = _cluster_apply._last_create_stderr
        error = stderr.strip() if stderr.strip() else "kubectl create failed (remove)"
        logger.warning(f"  remove apply 失败: {error[:200]}")
        return {
            "success": False,
            "error": error,
            "after_instr": None,
            "diff": {},
            "mutated_cr": None,
            "mutated_cr_yaml": mutated_cr_yaml,
            "apply_sec": apply_sec,
            "llm_sec": None,
            "blacklisted_vars": blacklisted_vars,
            "blacklisted_exprs": blacklisted_exprs,
        }


def _execute_single_mutation(
    field_path: str,
    field_present: bool,
    seed_cr: dict,
    namespace: str,
    base_cr: dict,
    base_instr: dict,
    field_base_cr_yaml: str,
    all_required_fields: List[str],
    crd_file: str,
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
    max_retries: int,
    instrument_prefix: str = "",
    declared_field_paths: Optional[set] = None,
    free_form_map_paths: Optional[set] = None,
    constraints_txt: str = "",
    project_path: str = "",
    instrument_dir: str = "",
    operator_error_logs: Optional[List[str]] = None,
) -> dict:
    """执行单次字段值变更：直接向 LLM 请求一个合法的替代值，apply 后做 diff。"""
    kind = "change" if field_present else "add"
    return _execute_change_step(
        step={},
        step_idx=0,
        kind=kind,
        field_path=field_path,
        seed_cr=seed_cr,
        namespace=namespace,
        base_cr=base_cr,
        base_instr=base_instr,
        field_base_cr_yaml=field_base_cr_yaml,
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
        free_form_map_paths=free_form_map_paths,
        constraints_txt=constraints_txt,
        project_path=project_path,
        instrument_dir=instrument_dir,
        use_direct_prompt=True,
        operator_error_logs=operator_error_logs,
    )


def _execute_change_step(
    step: dict,
    step_idx: int,
    kind: str,
    field_path: str,
    seed_cr: dict,
    namespace: str,
    base_cr: dict,
    base_instr: dict,
    field_base_cr_yaml: str,
    all_required_fields: List[str],
    crd_file: str,
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
    max_retries: int,
    instrument_prefix: str = "",
    declared_field_paths: Optional[set] = None,
    free_form_map_paths: Optional[set] = None,
    constraints_txt: str = "",
    project_path: str = "",
    instrument_dir: str = "",
    use_direct_prompt: bool = False,
    operator_error_logs: Optional[List[str]] = None,
) -> dict:
    """执行 change/add 步骤：plan patch 优先，失败则 LLM 重试。"""
    if operator_error_logs:
        _log_summary = "\n".join(operator_error_logs[:5])
        _src_ctx = _grep_project_for_error(operator_error_logs, project_path)
        error_feedback = (
            f"[上轮控制器错误日志]\n{_log_summary}\n"
            + (_src_ctx if _src_ctx else "")
            + "请确保生成的 CR 不会再触发上述错误。"
        )
        logger.info(
            f"  [operator-log] 注入错误上下文到 LLM ({len(operator_error_logs)} 条)"
        )
    else:
        error_feedback = ""

    llm_sec_accum: float = 0.0
    apply_sec_accum: float = 0.0
    sub_success = False
    sub_last_error = ""
    sub_after_instr: Optional[dict] = None
    sub_diff: dict = {}
    sub_mutated_cr: Optional[dict] = None
    sub_mutated_cr_yaml: str = ""

    plan_patch: Optional[dict] = None
    if "to" in step:
        _plan_set: dict = {field_path: step["to"]}
        _siblings = _extract_required_siblings(all_required_fields, field_path)
        _fbase_obj: Optional[dict] = None
        _unresolvable_siblings: list = []
        for _sib in _siblings:
            _norm_sib = re.sub(r"\[\d+\]", "[*]", _sib)
            _norm_tgt = re.sub(r"\[\d+\]", "[*]", field_path)
            if _norm_sib == _norm_tgt:
                continue
            if _get_current_field_value(base_cr, _sib) is not _FIELD_MISSING:
                continue
            if _fbase_obj is None:
                _fbase_obj = yaml.safe_load(field_base_cr_yaml) or {}
            _bval = _get_current_field_value(_fbase_obj, _sib)
            if _bval is not _FIELD_MISSING and _bval is not None:
                _plan_set[_sib] = _bval
            else:
                _unresolvable_siblings.append(_sib)
        if _unresolvable_siblings:
            logger.info(
                f"  ↪ 计划值无法补全必填兄弟字段 {_unresolvable_siblings}，改由 LLM 处理"
            )
            error_feedback = (
                f"You MUST set `{field_path}` to `{step['to']!r}` in this step. "
                f"Also ensure ALL required sibling fields are present: "
                + ", ".join(f"`{s}`" for s in _unresolvable_siblings)
            )
        else:
            if len(_plan_set) > 1:
                logger.info(f"  ↪ 计划补充必填兄弟字段: {list(_plan_set.keys())[1:]}")
            plan_patch = {"set": _plan_set, "delete": []}

    base_cr_yaml = yaml.dump(base_cr)

    for attempt_n in range(1, max_retries + 1):
        logger.info(f"  [{kind} 尝试 {attempt_n}/{max_retries}]")

        if plan_patch is not None and attempt_n == 1:
            patch = plan_patch
            patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
            if crd_err:
                error_feedback = crd_err
                sub_last_error = crd_err
                if not patch.get("set"):
                    plan_patch = None
                    continue
        else:
            if use_direct_prompt:
                field_present_now = (
                    _get_current_field_value(base_cr, field_path) is not _FIELD_MISSING
                )
                prompt = _build_direct_value_prompt(
                    field_path=field_path,
                    crd_file=crd_file,
                    cr_kind=cr_kind,
                    base_cr=base_cr,
                    field_present=field_present_now,
                    error_feedback=error_feedback,
                    constraints_txt=constraints_txt,
                )
            elif (
                kind == "add"
                and step_idx == 0
                and not _get_current_field_value(base_cr, field_path)
                is not _FIELD_MISSING
            ):
                prompt = _build_explore_add_prompt(
                    base_cr_yaml=base_cr_yaml,
                    field_path=field_path,
                    crd_file=crd_file,
                    cr_kind=cr_kind,
                    error_feedback=error_feedback,
                    base_cr=base_cr,
                    constraints_txt=constraints_txt,
                )
            else:
                prompt = _build_phase1_prompt(
                    base_cr_yaml=base_cr_yaml,
                    field_path=field_path,
                    crd_file=crd_file,
                    cr_kind=cr_kind,
                    error_feedback=error_feedback,
                    base_cr=base_cr,
                    constraints_txt=constraints_txt,
                )

            _prompt_siblings = _extract_required_siblings(
                all_required_fields, field_path
            )
            if _prompt_siblings:
                logger.info(
                    f"  [prompt] 必填兄弟字段 ({len(_prompt_siblings)}): "
                    f"{_prompt_siblings[:6]}{'...' if len(_prompt_siblings) > 6 else ''}"
                )

            _t_llm = _time.monotonic()
            with _timed_step("LLM 变异", field_path):
                action, new_cr_yaml = _call_llm_for_branch_flip(prompt)
            llm_sec_accum += _time.monotonic() - _t_llm
            logger.info(f"  [LLM 原始回复] {(new_cr_yaml or '').strip()[:400]}")

            if action == "error" or not new_cr_yaml:
                error_feedback = f"LLM error: {new_cr_yaml}"
                sub_last_error = error_feedback
                logger.warning(f"  LLM 失败: {error_feedback[:200]}")
                continue

            patch, parse_err = _parse_llm_patch(new_cr_yaml)
            if parse_err:
                error_feedback = (
                    f"Patch parse error: {parse_err}. "
                    "Output ONLY 'set:' and 'delete:' keys."
                )
                sub_last_error = error_feedback
                logger.warning(f"  Patch 解析失败: {parse_err}")
                continue
            patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
            if crd_err:
                error_feedback = crd_err
                sub_last_error = error_feedback
                if not patch.get("set"):
                    continue

        logger.info(f"  Patch: set={dict(patch['set'])} delete={patch['delete']}")
        try:
            mutated_cr = _apply_patch_to_cr(base_cr, patch)
            mutated_cr.setdefault("metadata", {})["name"] = seed_cr["metadata"]["name"]
            mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
            _field_base_cr_obj = yaml.safe_load(field_base_cr_yaml) or {}
            mutated_cr, _repaired = _repair_required_fields(
                mutated_cr, _field_base_cr_obj, all_required_fields, field_path
            )
            if _repaired:
                logger.info(f"  ↩ 修复缺失必填字段: {_repaired}")
            sub_mutated_cr_yaml = yaml.dump(mutated_cr)
        except Exception as e:
            error_feedback = f"Patch apply error: {e}"
            sub_last_error = error_feedback
            logger.warning(f"  Patch 应用失败: {e}")
            continue

        _new_val = _get_current_field_value(mutated_cr, field_path)
        _old_val = _get_current_field_value(base_cr, field_path)
        if (
            _new_val is not _FIELD_MISSING
            and _old_val is not _FIELD_MISSING
            and _new_val == _old_val
        ):
            _cur_repr = _fmt_current_value(base_cr, field_path)
            error_feedback = (
                f"No-op: the patch did not change `{field_path}`. "
                f"Its value is still `{_cur_repr}`. "
                f"You MUST choose a DIFFERENT value."
            )
            sub_last_error = error_feedback
            plan_patch = None
            logger.warning(f"  No-op patch skipped: {field_path} unchanged")
            continue

        _t_apply = _time.monotonic()
        after_instr_tmp, _, ok, _, _ = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=mutated_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
        )
        apply_sec_accum += _time.monotonic() - _t_apply

        if not ok or after_instr_tmp is None:
            stderr = _cluster_apply._last_create_stderr
            error_feedback = (
                stderr.strip() if stderr.strip() else "kubectl create failed"
            )
            sub_last_error = error_feedback
            plan_patch = None
            logger.warning(f"  apply/collect 失败: {error_feedback[:200]}")
            continue

        sub_diff = diff_branch_sequences(base_instr, after_instr_tmp)
        logger.info(
            f"  ✓ {kind} diff: changed={len(sub_diff.get('changed', []))}, "
            f"added={len(sub_diff.get('added', []))}, removed={len(sub_diff.get('removed', []))}"
        )

        from llm.runtime_constraints import (
            check_trace_shortfall,
            diagnose_and_fix_cr,
            find_divergence_branch,
            save_runtime_constraint,
        )

        if check_trace_shortfall(base_instr, after_instr_tmp):
            _div_bi = find_divergence_branch(base_instr, after_instr_tmp)
            if _div_bi is not None:
                _profile_dir = (
                    os.path.dirname(os.path.abspath(config_path)) if config_path else ""
                )
                _is_viol, _fixed_cr, _new_constraint = diagnose_and_fix_cr(
                    base_cr=base_cr,
                    mutated_cr=mutated_cr,
                    before_instr=base_instr,
                    after_instr=after_instr_tmp,
                    branch_index=_div_bi,
                    branch_meta_index=branch_meta_index,
                    project_path=project_path,
                    instrument_dir=instrument_dir,
                )
                if _is_viol:
                    if _new_constraint and _profile_dir:
                        save_runtime_constraint(_profile_dir, _new_constraint)
                    if _fixed_cr is not None:
                        logger.info("  [runtime] 使用修复后的 CR 重新 apply...")
                        _fixed_cr.setdefault("metadata", {})["name"] = seed_cr[
                            "metadata"
                        ]["name"]
                        _fixed_cr.setdefault("metadata", {}).setdefault(
                            "namespace", namespace
                        )
                        _rt_instr, _, _rt_ok, _, _ = apply_cr_and_collect(
                            kubectl_client=kubectl_client,
                            namespace=namespace,
                            cluster_name=cluster_name,
                            input_cr=_fixed_cr,
                            operator_container_name=operator_container_name,
                            wait_sec=wait_sec,
                            collect_max_wait=collect_max_wait,
                            instrument_prefix=instrument_prefix,
                        )
                        if _rt_ok and _rt_instr is not None:
                            logger.info(
                                f"  [runtime] 修复后轨迹长度: {len(_rt_instr.get('traces', []))}"
                            )
                            after_instr_tmp = _rt_instr
                            mutated_cr = _fixed_cr
                            sub_mutated_cr_yaml = yaml.dump(_fixed_cr)
                            sub_diff = diff_branch_sequences(
                                base_instr, after_instr_tmp
                            )

        _update_field_relations_from_diff(
            field_relations=field_relations,
            diff=sub_diff,
            cr_before=base_cr,
            cr_after=mutated_cr,
            mutation_round=f"explore-all-{field_path}-step{step_idx + 1}",
            branch_meta_index=branch_meta_index,
            blacklisted_vars=blacklisted_vars,
            blacklisted_exprs=blacklisted_exprs,
            declared_field_paths=declared_field_paths,
            free_form_map_paths=free_form_map_paths,
        )
        blacklisted_vars, blacklisted_exprs = _check_and_update_blacklist(
            field_relations, config_path, blacklisted_vars, blacklisted_exprs
        )
        sub_success = True
        sub_after_instr = after_instr_tmp
        sub_mutated_cr = mutated_cr
        sub_mutated_cr_yaml = yaml.dump(mutated_cr)
        break

    return {
        "success": sub_success,
        "error": sub_last_error,
        "after_instr": sub_after_instr,
        "diff": sub_diff,
        "mutated_cr": sub_mutated_cr,
        "mutated_cr_yaml": sub_mutated_cr_yaml,
        "llm_sec": round(llm_sec_accum, 2),
        "apply_sec": round(apply_sec_accum, 2),
        "blacklisted_vars": blacklisted_vars,
        "blacklisted_exprs": blacklisted_exprs,
    }


class _LoopContext:
    """Read-only configuration threaded through every loop iteration."""

    __slots__ = (
        "kubectl_client",
        "namespace",
        "cluster_name",
        "operator_container_name",
        "seed_cr",
        "crd_file",
        "cr_kind",
        "max_retries",
        "wait_sec",
        "collect_max_wait",
        "branch_meta_index",
        "config_path",
        "no_llm",
        "acto_input_model",
        "acto_seed_cr",
        "instrument_prefix",
        "constraints_data",
        "project_path",
        "instrument_dir",
        "all_required_fields",
        "free_form_map_paths",
        "declared_field_paths",
        "all_crd_paths",
        "baseline_trace_count",
        "nil_probe_enabled",
        "nil_probe_fields",
        "db_dir",
        "global_coverage_set",
    )

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _init_exploration_state(ckpt: dict, crd_fields: List[dict]) -> tuple:
    """Restore or create the 'explore_all' checkpoint sub-dict.

    Returns (ea, field_relations, completed_set, to_do, already_done_in_set).
    """
    ea = ckpt.setdefault(
        "explore_all",
        {"completed_fields": [], "mutation_log": [], "baseline_collected": False},
    )
    field_relations = ckpt.setdefault("field_relations", {})
    completed_set = set(ea["completed_fields"])
    to_do = [f for f in crd_fields if f["path"] not in completed_set]
    already_done_in_set = sum(1 for f in crd_fields if f["path"] in completed_set)
    return ea, field_relations, completed_set, to_do, already_done_in_set


def _build_loop_preconditions(
    ctx: "_LoopContext",
    field_relations: dict,
    config_path: str,
) -> tuple:
    """Compute blacklists, required fields, acto map (one-time setup).

    Returns (blacklisted_vars, blacklisted_exprs, all_required_fields,
             free_form_map_paths, acto_field_map).
    Raises RuntimeError if no-llm mode is requested without acto_input_model.
    """
    if config_path:
        _bl = _load_var_blacklist(config_path)
        blacklisted_vars: set = set(_bl.get("variables", []))
        blacklisted_exprs: set = set(_bl.get("expressions", []))
        if blacklisted_vars or blacklisted_exprs:
            logger.info(
                f"[blacklist] 已加载: {len(blacklisted_vars)} 个变量, "
                f"{len(blacklisted_exprs)} 个表达式"
            )
            _purge_blacklisted_from_relations(
                field_relations, blacklisted_vars, blacklisted_exprs
            )
    else:
        blacklisted_vars = set()
        blacklisted_exprs = set()

    all_required_fields: List[str] = _extract_crd_required_fields(
        ctx.crd_file, ctx.cr_kind
    )
    free_form_map_paths: set = _extract_free_form_map_paths(ctx.crd_file, ctx.cr_kind)

    acto_field_map: dict = {}
    if ctx.no_llm:
        if ctx.acto_input_model is None:
            logger.error("[no-llm] acto_input_model 未提供，退出")
            raise RuntimeError("acto_input_model is required in no-llm mode")
        acto_field_map = _build_acto_field_map(ctx.acto_input_model)
        logger.info(
            f"[no-llm] Acto 字段映射: {len(acto_field_map)} 个不同字段路径，"
            f"共 {sum(len(v) for v in acto_field_map.values())} 个测试用例"
        )

    return (
        blacklisted_vars,
        blacklisted_exprs,
        all_required_fields,
        free_form_map_paths,
        acto_field_map,
    )


def _run_field_mutation_nollm(
    field_path: str,
    ctx: "_LoopContext",
    step_base_cr: dict,
    step_base_instr: dict,
    acto_field_map: dict,
    field_relations: dict,
    blacklisted_vars: set,
    blacklisted_exprs: set,
) -> tuple:
    """Execute all acto test cases for a single field.

    Returns (field_success, sub_results, last_cr, last_instr,
             best_diff, best_after_instr, best_mutated_cr_yaml,
             blacklisted_vars, blacklisted_exprs).
    """
    _empty = (False, [], None, None, {}, None, "", blacklisted_vars, blacklisted_exprs)

    _norm_fp = _normalize_field_path_for_matching(field_path)
    _acto_tcs = acto_field_map.get(_norm_fp, [])
    if not _acto_tcs:
        logger.info(
            f"  [no-llm] 字段 '{field_path}' (norm='{_norm_fp}') 无 acto 测试用例，跳过"
        )
        return _empty

    test_plan_steps = [
        {"_tc": tc, "_path_str": ps, "rationale": f"acto:{repr(tc)[:60]}"}
        for ps, tc in _acto_tcs
    ]
    _acto_seed = ctx.acto_seed_cr if ctx.acto_seed_cr is not None else ctx.seed_cr

    field_success = False
    sub_results: list = []
    last_cr: Optional[dict] = None
    last_instr: Optional[dict] = None
    best_diff: dict = {}
    best_after_instr: Optional[dict] = None
    best_mutated_cr_yaml: str = ""
    current_base_cr = step_base_cr
    current_base_instr = step_base_instr

    for step_idx, step in enumerate(test_plan_steps):
        logger.info(
            f"  [step {step_idx + 1}/{len(test_plan_steps)}: acto  →  "
            f"{step.get('_path_str', '')[:40]}]"
        )
        _t = _time.monotonic()
        sr = _execute_acto_step(
            testcase=step["_tc"],
            path_str=step["_path_str"],
            acto_input_model=ctx.acto_input_model,
            seed_cr=_acto_seed,
            namespace=ctx.namespace,
            base_instr=current_base_instr,
            field_path=field_path,
            kubectl_client=ctx.kubectl_client,
            cluster_name=ctx.cluster_name,
            operator_container_name=ctx.operator_container_name,
            wait_sec=ctx.wait_sec,
            collect_max_wait=ctx.collect_max_wait,
            field_relations=field_relations,
            branch_meta_index=ctx.branch_meta_index,
            config_path=ctx.config_path,
            blacklisted_vars=blacklisted_vars,
            blacklisted_exprs=blacklisted_exprs,
            step_idx=step_idx,
            instrument_prefix=ctx.instrument_prefix,
            declared_field_paths=ctx.declared_field_paths,
            free_form_map_paths=ctx.free_form_map_paths,
        )
        blacklisted_vars = sr["blacklisted_vars"]
        blacklisted_exprs = sr["blacklisted_exprs"]
        sub_results.append(
            _build_sub_result(
                step_idx=step_idx,
                rationale=step.get("rationale", ""),
                kind="acto",
                sr=sr,
                base_cr=current_base_cr,
                timing_total=round(_time.monotonic() - _t, 2),
            )
        )
        if sr["success"]:
            field_success = True
            last_cr = sr["mutated_cr"]
            last_instr = sr["after_instr"]
            best_diff = sr["diff"]
            best_after_instr = sr["after_instr"]
            best_mutated_cr_yaml = sr["mutated_cr_yaml"]
            current_base_cr = sr["mutated_cr"]
            current_base_instr = sr["after_instr"]

    return (
        field_success,
        sub_results,
        last_cr,
        last_instr,
        best_diff,
        best_after_instr,
        best_mutated_cr_yaml,
        blacklisted_vars,
        blacklisted_exprs,
    )


def _run_field_mutation_llm(
    field_path: str,
    field_present: bool,
    ctx: "_LoopContext",
    step_base_cr: dict,
    step_base_instr: dict,
    field_base_cr_yaml: str,
    all_required_fields: List[str],
    field_relations: dict,
    blacklisted_vars: set,
    blacklisted_exprs: set,
    constraints_txt: str,
    operator_error_logs: Optional[List[str]],
) -> tuple:
    """Execute a single direct-value LLM mutation + optional add->change follow-up.

    Returns same shape as _run_field_mutation_nollm.
    """
    logger.info(
        f"  [baseline-cr] nodePort={_get_current_field_value(step_base_cr, 'spec.networking.nodePort')}"
    )
    _t = _time.monotonic()
    sr = _execute_single_mutation(
        field_path=field_path,
        field_present=field_present,
        seed_cr=ctx.seed_cr,
        namespace=ctx.namespace,
        base_cr=step_base_cr,
        base_instr=step_base_instr,
        field_base_cr_yaml=field_base_cr_yaml,
        all_required_fields=all_required_fields,
        crd_file=ctx.crd_file,
        cr_kind=ctx.cr_kind,
        kubectl_client=ctx.kubectl_client,
        cluster_name=ctx.cluster_name,
        operator_container_name=ctx.operator_container_name,
        wait_sec=ctx.wait_sec,
        collect_max_wait=ctx.collect_max_wait,
        field_relations=field_relations,
        branch_meta_index=ctx.branch_meta_index,
        config_path=ctx.config_path,
        blacklisted_vars=blacklisted_vars,
        blacklisted_exprs=blacklisted_exprs,
        max_retries=ctx.max_retries,
        instrument_prefix=ctx.instrument_prefix,
        declared_field_paths=ctx.declared_field_paths,
        free_form_map_paths=ctx.free_form_map_paths,
        constraints_txt=constraints_txt,
        project_path=ctx.project_path,
        instrument_dir=ctx.instrument_dir,
        operator_error_logs=operator_error_logs or None,
    )
    blacklisted_vars = sr["blacklisted_vars"]
    blacklisted_exprs = sr["blacklisted_exprs"]
    sub_results = [
        _build_sub_result(
            step_idx=0,
            rationale="direct value mutation",
            kind="change" if field_present else "add",
            sr=sr,
            base_cr=step_base_cr,
            timing_total=round(_time.monotonic() - _t, 2),
        )
    ]

    sub_success = sr["success"]
    sub_mutated_cr = sr["mutated_cr"]
    sub_mutated_cr_yaml = sr["mutated_cr_yaml"]
    sub_diff = sr["diff"]
    sub_after_instr = sr["after_instr"]


    if sub_success and not field_present and sub_mutated_cr is not None:
        logger.info(
            f"  [add->change] 字段 {field_path} 已成功添加，"
            f"追加 change 探测以建立变量级关联..."
        )
        _t2 = _time.monotonic()
        _fsr = _execute_single_mutation(
            field_path=field_path,
            field_present=True,
            seed_cr=ctx.seed_cr,
            namespace=ctx.namespace,
            base_cr=sub_mutated_cr,
            base_instr=sub_after_instr,
            field_base_cr_yaml=sub_mutated_cr_yaml,
            all_required_fields=all_required_fields,
            crd_file=ctx.crd_file,
            cr_kind=ctx.cr_kind,
            kubectl_client=ctx.kubectl_client,
            cluster_name=ctx.cluster_name,
            operator_container_name=ctx.operator_container_name,
            wait_sec=ctx.wait_sec,
            collect_max_wait=ctx.collect_max_wait,
            field_relations=field_relations,
            branch_meta_index=ctx.branch_meta_index,
            config_path=ctx.config_path,
            blacklisted_vars=blacklisted_vars,
            blacklisted_exprs=blacklisted_exprs,
            max_retries=ctx.max_retries,
            instrument_prefix=ctx.instrument_prefix,
            declared_field_paths=ctx.declared_field_paths,
            free_form_map_paths=ctx.free_form_map_paths,
            constraints_txt=constraints_txt,
            project_path=ctx.project_path,
            instrument_dir=ctx.instrument_dir,
            operator_error_logs=operator_error_logs or None,
        )
        blacklisted_vars = _fsr["blacklisted_vars"]
        blacklisted_exprs = _fsr["blacklisted_exprs"]
        sub_results.append(
            _build_sub_result(
                step_idx=1,
                rationale="add->change follow-up (build variable-level relation)",
                kind="change",
                sr=_fsr,
                base_cr=sub_mutated_cr,
                timing_total=round(_time.monotonic() - _t2, 2),
            )
        )
        if _fsr["success"] and _fsr["mutated_cr"] is not None:
            sub_success = True
            sub_mutated_cr = _fsr["mutated_cr"]
            sub_mutated_cr_yaml = _fsr.get("mutated_cr_yaml", "")
            sub_diff = _fsr["diff"]
            sub_after_instr = _fsr["after_instr"]
            logger.info(
                f"  [add->change] 成功: changed={len(sub_diff.get('changed', []))} "
                f"added={len(sub_diff.get('added', []))} "
                f"removed={len(sub_diff.get('removed', []))}"
            )
        else:
            logger.info(f"  [add->change] 失败: {_fsr.get('error', '')[:120]}")

    if sub_success:
        _log_mutation_success(
            field_path=field_path,
            step_base_cr=step_base_cr,
            sub_mutated_cr=sub_mutated_cr,
            sub_diff=sub_diff,
            field_relations=field_relations,
            free_form_map_paths=ctx.free_form_map_paths,
        )
    else:
        logger.info(f"  ✗ 变异失败: {sr.get('error', '')[:120]}")

    last_cr = sub_mutated_cr if sub_success else None
    last_instr = sub_after_instr if sub_success else None

    return (
        sub_success,
        sub_results,
        last_cr,
        last_instr,
        sub_diff if sub_success else {},
        sub_after_instr if sub_success else None,
        sub_mutated_cr_yaml if sub_success else "",
        blacklisted_vars,
        blacklisted_exprs,
    )


def _should_run_nil_probe(
    field_path: str,
    nil_probe_enabled: bool,
    nil_probe_fields: Optional[set],
) -> bool:
    """Decide whether to run a nil probe for this field.

    - nil_probe_enabled=False  -> never probe.
    - nil_probe_fields=None    -> probe ALL fields (when enabled).
    - nil_probe_fields=<set>   -> probe only the listed fields.
    """
    if not nil_probe_enabled:
        return False
    if nil_probe_fields is None:
        return True
    return field_path in nil_probe_fields


def _log_nil_probe_result(
    field_path: str,
    base_cr: dict,
    probe_diff: dict,
    field_relations: dict,
) -> None:
    """Emit a detailed log for a nil-probe result, mirroring _log_mutation_success.

    nil probe 只关注单一字段整体有无，因此：
    - 固定显示 "(present) -> (absent)" 作为值变更描述
    - 只展示 changed 分支中的变量 diff（与主变异日志格式完全一致）
    - 显示本次探测为该字段新增了多少分支关联
    - 不涉及旁路字段分析
    """
    _diff_changed = len(probe_diff.get("changed", []))
    _diff_added = len(probe_diff.get("added", []))
    _diff_removed = len(probe_diff.get("removed", []))

    _fr_now = field_relations.get(field_path, {})
    _n_branch_rel = len(_fr_now.get("branch_indices", []))
    _recorded_bis: set = set(_fr_now.get("branch_indices", []))


    _branch_lines: list = []
    for _brec in probe_diff.get("changed", []):
        _bi = _brec.get("branch_index", "?")
        _bval = _brec.get("before_value")
        _aval = _brec.get("after_value")
        _bool_part = f"  取值: {_bval} -> {_aval}" if _bval != _aval else ""
        if "entry_diffs" in _brec:
            _vd: dict = {}
            for _ed in _brec["entry_diffs"]:
                for _k, _v in _ed.get("variables_diff", {}).items():
                    _prev = _vd.get(_k)
                    _vd[_k] = (
                        _v
                        if _prev is None
                        else {**_v, "before_value": _prev["before_value"]}
                    )
        else:
            _vd = _brec.get("variables_diff", {})
        _var_parts = [
            f"{_v.get('variable_fmt') or _v.get('variable_id', '?')}: "
            f"{repr(_v.get('before_value'))[:40]} -> {repr(_v.get('after_value'))[:40]}"
            for _v in _vd.values()
        ]
        _var_str = "  |  ".join(_var_parts[:4]) + (
            f"  … (+{len(_var_parts) - 4})" if len(_var_parts) > 4 else ""
        )
        _mark = "✓" if _bi in _recorded_bis else "○"
        _branch_lines.append(
            f"      [{_mark}] b[{_bi}]{_bool_part}"
            + (f"  变量: {_var_str}" if _var_str else "  (无变量 diff)")
        )

    _branch_detail = (
        "\n".join(_branch_lines) if _branch_lines else "      (无 changed 分支)"
    )

    logger.info(
        f"  [nil-probe] ✓ 删除探测完成\n"
        f"    目标字段  : {field_path}\n"
        f"    值变更    : (present)  ->  (absent)\n"
        f"    分支变化  : changed={_diff_changed}  added={_diff_added}  removed={_diff_removed}\n"
        f"{_branch_detail}\n"
        f"    累计关联分支: {_n_branch_rel} 个  (字段 {field_path}，含主变异)"
    )


def _run_nil_probe(
    field_path: str,
    field_present: bool,
    ctx: "_LoopContext",
    base_cr: dict,
    base_instr: dict,
    field_base_cr_yaml: str,
    field_relations: dict,
    blacklisted_vars: set,
    blacklisted_exprs: set,
) -> tuple:
    """Optional nil/delete probe: remove the field and observe branch changes.

    目的
    ----
    覆盖算子内部封装函数（如 ``IsNodePortEnabled()``）中对字段是否为 nil 的隐式
    检查。纯粹的值变更测试保持字段存在，无法触发 ``field == nil`` 这一路径。
    删除字段后重新 apply 可以强制走 nil 分支，从而建立字段缺失与分支翻转的关联。

    语义约束
    --------
    - 始终以主变异*之前*的 CR（current_cr / current_instr）为对比基准，
      使 diff 只反映"字段从有到无"这一单一变化的效果。
    - 探测结果**不参与旁路字段分析**：nil probe 对应"整体删除"语义，
      不适合用来推断子字段的独立关联。旁路分析由调用方在主变异结果上单独进行。

    Returns
    -------
    (probe_result | None, blacklisted_vars, blacklisted_exprs)

    ``probe_result`` 与 sub_result 结构相同，可直接追加到 sub_results。
    字段本来就不存在时返回 None（跳过探测）。
    """
    if not field_present:
        logger.info(f"  [nil-probe] 跳过: 字段 {field_path} 在变异前已不存在")
        return None, blacklisted_vars, blacklisted_exprs

    logger.info(f"  [nil-probe] 开始删除探测: {field_path}")
    _t = _time.monotonic()
    sr = _execute_remove_step(
        field_path=field_path,
        seed_cr=ctx.seed_cr,
        namespace=ctx.namespace,
        base_cr=base_cr,
        base_instr=base_instr,
        field_base_cr_yaml=field_base_cr_yaml,
        kubectl_client=ctx.kubectl_client,
        cluster_name=ctx.cluster_name,
        operator_container_name=ctx.operator_container_name,
        wait_sec=ctx.wait_sec,
        collect_max_wait=ctx.collect_max_wait,
        field_relations=field_relations,
        branch_meta_index=ctx.branch_meta_index,
        config_path=ctx.config_path,
        blacklisted_vars=blacklisted_vars,
        blacklisted_exprs=blacklisted_exprs,
        instrument_prefix=ctx.instrument_prefix,
        declared_field_paths=ctx.declared_field_paths,
        free_form_map_paths=ctx.free_form_map_paths,
    )
    blacklisted_vars = sr["blacklisted_vars"]
    blacklisted_exprs = sr["blacklisted_exprs"]
    _total = round(_time.monotonic() - _t, 2)

    if sr["success"]:
        _log_nil_probe_result(
            field_path=field_path,
            base_cr=base_cr,
            probe_diff=sr["diff"],
            field_relations=field_relations,
        )
    else:
        logger.info(f"  [nil-probe] ✗ 删除探测失败: {sr.get('error', '')[:120]}")

    result = _build_sub_result(
        step_idx=-1,
        rationale="nil-probe (delete field to cover nil-check branches)",
        kind="remove",
        sr=sr,
        base_cr=base_cr,
        timing_total=_total,
    )
    return result, blacklisted_vars, blacklisted_exprs


def _update_rolling_baseline(
    ctx: "_LoopContext",
    ea: dict,
    field_path: str,
    field_success: bool,
    last_successful_cr: Optional[dict],
    last_successful_instr: Optional[dict],
    current_cr: dict,
    current_instr: dict,
) -> tuple:
    """Advance or reset the rolling baseline depending on trace health.

    Returns updated (current_cr, current_instr).
    """
    if not (
        field_success
        and last_successful_cr is not None
        and last_successful_instr is not None
    ):
        return current_cr, current_instr

    if _check_trace_health(last_successful_instr, ctx.baseline_trace_count):
        current_instr = last_successful_instr
        current_cr = last_successful_cr
        ea["current_instr"] = current_instr
        ea["current_cr_yaml"] = yaml.dump(current_cr)
        if ctx.config_path:
            _update_branch_baseline_crs(
                ctx.config_path,
                last_successful_instr,
                yaml.dump(last_successful_cr),
            )
    else:
        logger.warning(
            f"[health] 字段 {field_path} 结果 trace 异常，回退滚动基准到 seed CR"
        )
        _seed_instr, _, _reset_ok, _, _ = apply_cr_and_collect(
            kubectl_client=ctx.kubectl_client,
            namespace=ctx.namespace,
            cluster_name=ctx.cluster_name,
            input_cr=ctx.seed_cr,
            operator_container_name=ctx.operator_container_name,
            wait_sec=ctx.wait_sec,
            collect_max_wait=ctx.collect_max_wait,
            instrument_prefix=ctx.instrument_prefix,
        )
        if _reset_ok and _seed_instr is not None:
            current_instr = _seed_instr
            current_cr = ctx.seed_cr
            ea["current_instr"] = current_instr
            ea["current_cr_yaml"] = yaml.dump(current_cr)
            logger.info("[health] seed CR 重置成功，滚动基准已恢复")
        else:
            logger.error("[health] seed CR 重置失败，保持上一次基准不变")

    return current_cr, current_instr


def _write_field_checkpoint(
    ea: dict,
    ckpt: dict,
    ckpt_path: str,
    completed_set: set,
    field_path: str,
    field_info: dict,
    field_present: bool,
    field_success: bool,
    sub_results: list,
    field_base_cr_yaml: str,
    best_diff: dict,
    best_after_instr: Optional[dict],
    best_mutated_cr_yaml: str,
    operator_error_logs: List[str],
    t_field_start: float,
) -> None:
    """Aggregate sub-results into ea['mutation_log'] and persist checkpoint."""
    all_cr_changed: list = []
    for sr in sub_results:
        for fp in sr.get("cr_changed_fields", []):
            if fp not in all_cr_changed:
                all_cr_changed.append(fp)

    _total_sec = round(_time.monotonic() - t_field_start, 2)
    _llm_sec = round(
        sum((sr.get("timing") or {}).get("llm_sec") or 0 for sr in sub_results), 2
    )
    _apply_sec = round(
        sum((sr.get("timing") or {}).get("apply_sec") or 0 for sr in sub_results), 2
    )

    ea["completed_fields"].append(field_path)
    ea["mutation_log"].append(
        {
            "field": field_path,
            "field_type": field_info.get("type", ""),
            "field_depth": field_info.get("depth", 0),
            "collateral_of": field_info.get("_collateral_of"),
            "status": "ok" if field_success else "failed",
            "error": (
                ""
                if field_success
                else (
                    sub_results[-1]["error"]
                    if sub_results
                    else "no sub-mutations generated"
                )
            ),
            "field_present": field_present,
            "test_plan": {
                "raw_llm_response": "",
                "steps": [],
                "plan_error": "",
                "plan_llm_sec": 0.0,
            },
            "sub_mutations": sub_results,
            "cr_changed_fields": all_cr_changed,
            "diff_summary": (
                {
                    "changed": len(best_diff.get("changed", [])),
                    "added": len(best_diff.get("added", [])),
                    "removed": len(best_diff.get("removed", [])),
                }
                if field_success
                else {}
            ),
            "operator_error_logs": operator_error_logs,
            "diff_raw": best_diff if field_success else None,
            "after_instr": best_after_instr if field_success else None,
            "base_cr_yaml": field_base_cr_yaml,
            "mutated_cr_yaml": best_mutated_cr_yaml,
            "timing": {
                "total_sec": _total_sec,
                "llm_sec": _llm_sec,
                "apply_sec": _apply_sec,
            },
        }
    )
    completed_set.add(field_path)
    _save_checkpoint(ckpt_path, ckpt)


def _build_sub_result(
    step_idx: int,
    rationale: str,
    kind: str,
    sr: dict,
    base_cr: dict,
    timing_total: float,
) -> dict:
    """Build the sub_result dict that goes into mutation_log['sub_mutations']."""
    sub_success = sr["success"]
    sub_mutated_cr = sr.get("mutated_cr")
    sub_diff = sr.get("diff", {})
    return {
        "step_idx": step_idx + 1,
        "step_rationale": rationale,
        "step_to": None,
        "kind": kind,
        "status": "ok" if sub_success else "failed",
        "error": "" if sub_success else sr.get("error", ""),
        "diff_summary": (
            {
                "changed": len(sub_diff.get("changed", [])),
                "added": len(sub_diff.get("added", [])),
                "removed": len(sub_diff.get("removed", [])),
            }
            if sub_success
            else {}
        ),
        "cr_changed_fields": (
            _cr_changed_fields(base_cr, sub_mutated_cr)
            if sub_success and sub_mutated_cr is not None
            else []
        ),
        "base_cr_yaml": yaml.dump(base_cr),
        "mutated_cr_yaml": sr.get("mutated_cr_yaml", ""),
        "timing": {
            "llm_sec": sr.get("llm_sec"),
            "apply_sec": sr.get("apply_sec"),
            "total_sec": timing_total,
        },
    }


def _log_mutation_success(
    field_path: str,
    step_base_cr: dict,
    sub_mutated_cr: Optional[dict],
    sub_diff: dict,
    field_relations: dict,
    free_form_map_paths: set,
) -> None:
    _changed_fps = (
        _collapse_free_form_sub_paths(
            _cr_changed_fields(step_base_cr, sub_mutated_cr),
            free_form_map_paths,
        )
        if sub_mutated_cr
        else []
    )
    _diff_changed = len(sub_diff.get("changed", []))
    _diff_added = len(sub_diff.get("added", []))
    _diff_removed = len(sub_diff.get("removed", []))
    _fp_disp = ", ".join(_changed_fps[:5]) + (
        f" … (+{len(_changed_fps) - 5})" if len(_changed_fps) > 5 else ""
    )
    _before_val = _get_current_field_value(step_base_cr, field_path)
    _after_val = (
        _get_current_field_value(sub_mutated_cr, field_path)
        if sub_mutated_cr
        else _FIELD_MISSING
    )
    _bv_s = repr(_before_val)[:60] if _before_val is not _FIELD_MISSING else "(absent)"
    _av_s = repr(_after_val)[:60] if _after_val is not _FIELD_MISSING else "(absent)"
    _fr_now = field_relations.get(field_path, {})
    _n_branch_rel = len(_fr_now.get("branch_indices", []))
    _recorded_bis: set = set(_fr_now.get("branch_indices", []))
    _n_cr_changed = len(_changed_fps)
    _tracker_skipped = _n_cr_changed != 1

    _branch_lines: list = []
    for _brec in sub_diff.get("changed", []):
        _bi = _brec.get("branch_index", "?")
        _bval = _brec.get("before_value")
        _aval = _brec.get("after_value")
        _bool_part = f"  取值: {_bval} -> {_aval}" if _bval != _aval else ""
        if "entry_diffs" in _brec:
            _vd: dict = {}
            for _ed in _brec["entry_diffs"]:
                for _k, _v in _ed.get("variables_diff", {}).items():
                    _prev = _vd.get(_k)
                    _vd[_k] = (
                        _v
                        if _prev is None
                        else {**_v, "before_value": _prev["before_value"]}
                    )
        else:
            _vd = _brec.get("variables_diff", {})
        _var_parts = [
            f"{_v.get('variable_fmt') or _v.get('variable_id', '?')}: "
            f"{repr(_v.get('before_value'))[:40]} -> {repr(_v.get('after_value'))[:40]}"
            for _v in _vd.values()
        ]
        _var_str = "  |  ".join(_var_parts[:4]) + (
            f"  … (+{len(_var_parts) - 4})" if len(_var_parts) > 4 else ""
        )
        _mark = "✓" if _bi in _recorded_bis else "○"
        _branch_lines.append(
            f"      [{_mark}] b[{_bi}]{_bool_part}"
            + (f"  变量: {_var_str}" if _var_str else "  (无变量 diff)")
        )

    _branch_detail = (
        "\n".join(_branch_lines) if _branch_lines else "      (无 changed 分支)"
    )
    if _tracker_skipped:
        _skip_note = (
            f"\n    ⚠ 关联未记录: CR 同时变化了 {_n_cr_changed} 个叶子字段"
            f"（需精确变更 1 个字段才可建立关联）"
        )
    elif _diff_added and not _n_branch_rel:
        _skip_note = f"\n    ℹ 新增分支 {_diff_added} 个（从无到有，不纳入关联）"
    else:
        _skip_note = ""

    logger.info(
        f"  ✓ 变异成功\n"
        f"    目标字段  : {field_path}\n"
        f"    值变更    : {_bv_s}  ->  {_av_s}\n"
        f"    CR 变更字段: [{_fp_disp}] ({_n_cr_changed} 个)\n"
        f"    分支变化  : changed={_diff_changed}  added={_diff_added}  removed={_diff_removed}\n"
        f"{_branch_detail}"
        f"{_skip_note}\n"
        f"    累计关联分支: {_n_branch_rel} 个  (字段 {field_path})"
    )


def run_full_field_exploration(
    ckpt: dict,
    ckpt_path: str,
    kubectl_client: KubectlClient,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    seed_cr: dict,
    crd_file: str,
    cr_kind: str,
    crd_fields: List[dict],
    max_retries: int = 3,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    branch_meta_index: Optional[dict] = None,
    config_path: str = "",
    no_llm: bool = False,
    acto_input_model=None,
    acto_seed_cr: Optional[dict] = None,
    instrument_prefix: str = "",
    constraints_data: Optional[dict] = None,
    project_path: str = "",
    instrument_dir: str = "",

    nil_probe_enabled: bool = True,
    nil_probe_fields: Optional[set] = None,

    db_dir: str = "",
):
    """探索所有 CRD 字段，建立 field->branch 关联映射，支持断点续传。

    主循环只负责调度，所有业务逻辑委托给独立的辅助函数。

    Nil-probe 参数
    --------------
    nil_probe_enabled : bool
        是否启用 nil/删除探测（默认关闭）。开启后对每个字段额外执行一次删除
        测试，覆盖封装函数内部的 nil-check 分支（如 IsNodePortEnabled 内部的
        ``!= nil`` 判断）。每次探测以*主变异之前*的 CR 为基准，不影响滚动基准。

    nil_probe_fields : set[str] | None
        为 None 时探测所有字段；提供集合则只探测集合内的字段，其余跳过。
        适合在已知含 nil-check 封装的字段上精准开启，控制额外开销。
    """
    logger.info("=" * 70)
    logger.info("Explore-All — 全量字段关联分析")
    logger.info("=" * 70)
    update_status(phase="Explore-All", current_op="初始化")


    ea, field_relations, completed_set, to_do, already_done_in_set = (
        _init_exploration_state(ckpt, crd_fields)
    )
    total = len(crd_fields)
    logger.info(
        f"CRD 字段总数: {total}, 已完成: {already_done_in_set}, 本次: {len(to_do)}"
    )
    if not to_do:
        logger.info("所有字段均已探索完毕")
        return

    seed_cr_yaml = yaml.dump(seed_cr)
    declared_field_paths: set = {f["path"] for f in crd_fields}
    all_crd_paths: dict = {f["path"]: f for f in crd_fields}


    baseline_instr = _collect_exploration_baseline(
        ea=ea,
        ckpt=ckpt,
        ckpt_path=ckpt_path,
        seed_cr=seed_cr,
        seed_cr_yaml=seed_cr_yaml,
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        operator_container_name=operator_container_name,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        config_path=config_path,
        instrument_prefix=instrument_prefix,
    )
    if baseline_instr is None:
        return
    current_instr, current_cr = _restore_rolling_baseline(ea, seed_cr)


    _global_coverage_set: Set[int] = set()
    if db_dir:
        try:
            from testcase_db.store import _load_index as _db_load_index

            _db_idx = _db_load_index(db_dir)
            _global_coverage_set = {int(k) for k in _db_idx.keys()}
            logger.info(
                f"[testcase_db] 已加载全局覆盖集合: {len(_global_coverage_set)} 个已覆盖分支"
            )
        except Exception as _dbe:
            logger.warning(f"[testcase_db] 加载全局覆盖集合失败: {_dbe}")

    ctx = _LoopContext(
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        operator_container_name=operator_container_name,
        seed_cr=seed_cr,
        crd_file=crd_file,
        cr_kind=cr_kind,
        max_retries=max_retries,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        branch_meta_index=branch_meta_index,
        config_path=config_path,
        no_llm=no_llm,
        acto_input_model=acto_input_model,
        acto_seed_cr=acto_seed_cr,
        instrument_prefix=instrument_prefix,
        constraints_data=constraints_data or {},
        project_path=project_path,
        instrument_dir=instrument_dir,
        all_required_fields=[],
        free_form_map_paths=set(),
        declared_field_paths=declared_field_paths,
        all_crd_paths=all_crd_paths,
        baseline_trace_count=len((baseline_instr or {}).get("traces", [])),
        nil_probe_enabled=nil_probe_enabled,
        nil_probe_fields=nil_probe_fields,
        db_dir=db_dir,
        global_coverage_set=_global_coverage_set,
    )

    try:
        (
            blacklisted_vars,
            blacklisted_exprs,
            all_required_fields,
            free_form_map_paths,
            acto_field_map,
        ) = _build_loop_preconditions(ctx, field_relations, config_path)
    except RuntimeError:
        return

    ctx.all_required_fields = all_required_fields
    ctx.free_form_map_paths = free_form_map_paths

    logger.info(f"[health] baseline trace 数量: {ctx.baseline_trace_count}")
    if nil_probe_enabled:
        _probe_scope = (
            f"{len(nil_probe_fields)} 个指定字段" if nil_probe_fields else "全部字段"
        )
        logger.info(f"[nil-probe] 已启用，作用范围: {_probe_scope}")


    from llm.constraints import filter_constraints, format_constraints_section


    _sorted_todo = sorted(
        to_do, key=lambda f: (f.get("depth", f["path"].count(".")), f["path"])
    )
    _work_queue: deque = deque(_sorted_todo)
    _operator_error_log_stash: List[str] = []
    idx = -1
    _db_recorded_count = 0

    _demoted_prefixes: set = set()

    while _work_queue:
        field_info = _work_queue.popleft()
        idx += 1
        field_path = field_info["path"]

        field_relations.setdefault(field_path, {})["field_type"] = field_info["type"]
        global_idx = already_done_in_set + idx + 1


        _dynamic_total = already_done_in_set + idx + 1 + len(_work_queue)
        update_status(current_op=field_path)
        update_progress(
            done=global_idx - 1,
            total=_dynamic_total,
            label="字段",
            relations=len(field_relations),
        )
        logger.info(f"\n[explore-all {global_idx}/{_dynamic_total}] 字段: {field_path}")
        logger.info(f"  (滚动基准: {len(current_instr.get('traces', []))} traces)")


        if db_dir and ctx.branch_meta_index:
            _fr_field = field_relations.get(field_path, {})
            _known_bis: Set[int] = set(_fr_field.get("branch_indices", []))
            _all_bis: Set[int] = set(ctx.branch_meta_index.keys())
            _unreached = _all_bis - _known_bis - _global_coverage_set
            if _unreached:
                try:
                    from testcase_db.store import query_covering_branch

                    _target_bi = next(iter(sorted(_unreached)))
                    _db_tcs = query_covering_branch(db_dir, _target_bi)
                    if _db_tcs:
                        _db_tc = _db_tcs[0]
                        _db_cr = yaml.safe_load(_db_tc.get("cr", "") or "") or None
                        if _db_cr is not None:
                            logger.info(
                                f"  [testcase_db] 发现 DB 用例 {_db_tc['id']} 覆盖未关联分支 b[{_target_bi}]，"
                                f"以其作为本字段变异基准"
                            )
                            current_cr = _db_cr
                            current_instr = (
                                current_instr
                            )
                except Exception as _dbe:
                    logger.debug(f"  [testcase_db] DB 基准查询失败: {_dbe}")

        field_present = _field_exists_in_cr(current_cr, field_path)
        _relevant = filter_constraints(ctx.constraints_data, [field_path])
        constraints_txt = format_constraints_section(_relevant)
        field_base_cr_yaml = yaml.dump(current_cr)
        _log_line_before = get_operator_log_line_count(
            kubectl_client, namespace, operator_container_name
        )
        _t_field_start = _time.monotonic()


        if no_llm:
            (
                field_success,
                sub_results,
                last_successful_cr,
                last_successful_instr,
                best_diff,
                best_after_instr,
                best_mutated_cr_yaml,
                blacklisted_vars,
                blacklisted_exprs,
            ) = _run_field_mutation_nollm(
                field_path=field_path,
                ctx=ctx,
                step_base_cr=current_cr,
                step_base_instr=current_instr,
                acto_field_map=acto_field_map,
                field_relations=field_relations,
                blacklisted_vars=blacklisted_vars,
                blacklisted_exprs=blacklisted_exprs,
            )
        else:
            (
                field_success,
                sub_results,
                last_successful_cr,
                last_successful_instr,
                best_diff,
                best_after_instr,
                best_mutated_cr_yaml,
                blacklisted_vars,
                blacklisted_exprs,
            ) = _run_field_mutation_llm(
                field_path=field_path,
                field_present=field_present,
                ctx=ctx,
                step_base_cr=current_cr,
                step_base_instr=current_instr,
                field_base_cr_yaml=field_base_cr_yaml,
                all_required_fields=all_required_fields,
                field_relations=field_relations,
                blacklisted_vars=blacklisted_vars,
                blacklisted_exprs=blacklisted_exprs,
                constraints_txt=constraints_txt,
                operator_error_logs=_operator_error_log_stash or None,
            )


        _this_round_has_relation = bool(best_diff.get("changed"))
        if (
            not no_llm
            and field_success
            and last_successful_cr is not None
            and _this_round_has_relation
        ):
            _collateral = _collect_collateral_fields(
                field_path=field_path,
                sub_mutated_cr=last_successful_cr,
                base_cr=current_cr,
                sub_diff=best_diff,
                declared_field_paths=declared_field_paths,
                completed_set=completed_set,
                free_form_map_paths=free_form_map_paths,
            )
            if _collateral:
                logger.info(
                    f"  [collateral] 检测到 {len(_collateral)} 个旁路字段，"
                    f"插入高优先探测队列: {_collateral}"
                )
                for _cfp in reversed(_collateral):
                    _cfi = all_crd_paths.get(_cfp) or {
                        "path": _cfp,
                        "type": "unknown",
                        "depth": _cfp.count("."),
                    }
                    if _cfp not in completed_set:
                        _work_queue.appendleft({**_cfi, "_collateral_of": field_path})
                        declared_field_paths.add(_cfp)
        elif (
            not no_llm
            and field_success
            and last_successful_cr is not None
            and not _this_round_has_relation
        ):
            logger.info("  [collateral] 本轮无 changed 分支关联，跳过旁路字段注入")


        _n_diff_changed = len(best_diff.get("changed", []))
        if _n_diff_changed == 0 and field_path not in _demoted_prefixes:
            _child_prefix1 = field_path + "."
            _child_prefix2 = field_path + "["
            _to_demote = [
                fi
                for fi in list(_work_queue)
                if fi["path"].startswith(_child_prefix1)
                or fi["path"].startswith(_child_prefix2)
            ]
            if _to_demote:
                _demoted_paths = {fi["path"] for fi in _to_demote}

                _remaining = [
                    fi for fi in _work_queue if fi["path"] not in _demoted_paths
                ]
                _work_queue = deque(_remaining)
                for fi in _to_demote:
                    _work_queue.append(fi)
                _demoted_prefixes.add(field_path)
                logger.info(
                    f"  [priority] 字段 {field_path} 无关联分支，"
                    f"将 {len(_to_demote)} 个子字段降至队列末尾: "
                    + ", ".join(fi["path"] for fi in _to_demote[:5])
                    + (f" … (+{len(_to_demote) - 5})" if len(_to_demote) > 5 else "")
                )


        if _should_run_nil_probe(
            field_path, ctx.nil_probe_enabled, ctx.nil_probe_fields
        ):
            _probe_result, blacklisted_vars, blacklisted_exprs = _run_nil_probe(
                field_path=field_path,
                field_present=field_present,
                ctx=ctx,
                base_cr=current_cr,
                base_instr=current_instr,
                field_base_cr_yaml=field_base_cr_yaml,
                field_relations=field_relations,
                blacklisted_vars=blacklisted_vars,
                blacklisted_exprs=blacklisted_exprs,
            )
            if _probe_result is not None:
                _probe_result["step_idx"] = len(sub_results) + 1
                sub_results.append(_probe_result)


        _operator_error_log_stash = fetch_operator_error_logs(
            kubectl_client,
            namespace,
            operator_container_name,
            skip_lines=_log_line_before,
        )
        if _operator_error_log_stash:
            logger.warning(
                f"[operator-log] 本轮新增 {len(_operator_error_log_stash)} 条错误/警告:\n"
                + "\n".join(f"  {m}" for m in _operator_error_log_stash[:5])
            )


        current_cr, current_instr = _update_rolling_baseline(
            ctx=ctx,
            ea=ea,
            field_path=field_path,
            field_success=field_success,
            last_successful_cr=last_successful_cr,
            last_successful_instr=last_successful_instr,
            current_cr=current_cr,
            current_instr=current_instr,
        )


        if (
            db_dir
            and field_success
            and best_after_instr is not None
            and best_mutated_cr_yaml
        ):
            try:
                from testcase_db.store import record_testcase

                _tc_id = record_testcase(
                    db_dir=db_dir,
                    cr_yaml=best_mutated_cr_yaml,
                    instr_data=best_after_instr,
                    source="explore_all",
                    global_coverage_set=ctx.global_coverage_set,
                )
                if _tc_id:
                    _db_recorded_count += 1

                    from testcase_db.store import _extract_covered_branches

                    for _bi in _extract_covered_branches(best_after_instr):
                        ctx.global_coverage_set.add(_bi)
                    logger.info(
                        f"  [testcase_db] 已记录用例 {_tc_id} "
                        f"(本次共记录 {_db_recorded_count} 个)"
                    )
            except Exception as _dbe:
                logger.warning(f"  [testcase_db] 记录失败: {_dbe}")


        _write_field_checkpoint(
            ea=ea,
            ckpt=ckpt,
            ckpt_path=ckpt_path,
            completed_set=completed_set,
            field_path=field_path,
            field_info=field_info,
            field_present=field_present,
            field_success=field_success,
            sub_results=sub_results,
            field_base_cr_yaml=field_base_cr_yaml,
            best_diff=best_diff,
            best_after_instr=best_after_instr,
            best_mutated_cr_yaml=best_mutated_cr_yaml,
            operator_error_logs=_operator_error_log_stash,
            t_field_start=_t_field_start,
        )


    n_relations = len(field_relations)
    n_done = len(ea["completed_fields"])
    n_ok = sum(1 for m in ea["mutation_log"] if m["status"] == "ok")
    logger.info(
        f"\nExplore-All 完成: {n_done}/{total} 字段已探索, "
        f"成功 {n_ok}, field_relations 涵盖 {n_relations} 个字段"
    )
    if db_dir:
        logger.info(
            f"  [testcase_db] 本次 Explore-All 共记录 {_db_recorded_count} 个新测试用例到数据库"
        )


def parse_llm_test_plan(raw: str) -> tuple:
    """Parse LLM test-plan YAML.  Returns (steps_list, error_str)."""
    try:
        parsed = yaml.safe_load(raw)
    except Exception as e:
        return [], f"YAML parse error: {e}"
    if not isinstance(parsed, dict):
        if isinstance(parsed, list):
            steps = parsed
        else:
            return [], f"Expected mapping with 'steps' key, got {type(parsed).__name__}"
    else:
        steps = parsed.get("steps", [])
    if not isinstance(steps, list) or not steps:
        return [], "No steps found in test plan"
    validated = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        if step.get("remove"):
            validated.append(
                {"remove": True, "rationale": str(step.get("rationale", ""))}
            )
        elif "to" in step:
            validated.append(
                {"to": step["to"], "rationale": str(step.get("rationale", ""))}
            )
    if not validated:
        return [], "All steps were malformed"
    return validated, ""