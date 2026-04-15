import logging
import time as _time
from typing import List, Optional

import yaml

import cluster.apply as _cluster_apply
from cluster.apply import apply_cr_and_collect
from core.cr_utils import (
    _cr_changed_fields,
)
from core.patch import _apply_patch_to_cr, _parse_llm_patch
from core.timing import _timed_step
from crd.validation import _validate_patch_against_crd
from instrumentation.diff import (
    diff_branch_sequences,
)
from instrumentation.source import _get_branch_source_context
from llm.client import _call_llm_for_branch_flip
from llm.constraints import filter_constraints, format_constraints_section
from llm.prompts import (
    _build_branch_flip_prompt,
    _related_fields_for_branch,
)

logger = logging.getLogger(__name__)


def run_branch_coverage_test(
    targets: List[dict],
    ckpt: dict,
    kubectl_client,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    seed_cr: dict,
    crd_file: str,
    cr_kind: str,
    branch_meta_index: dict,
    wait_sec: int = 15,
    collect_max_wait: int = 30,
    max_retries: int = 3,
    project_path: str = "",
    instrument_dir: str = "",
    include_source_code: bool = False,
    instrument_prefix: str = "",
    constraints_data: Optional[dict] = None,
) -> List[dict]:
    """For each (branch_index, target_value) target, attempt to generate a CR that
    achieves the target branch value, apply it, verify the outcome, and return results.

    Returns a list of result dicts — one per target.
    """
    field_relations: dict = ckpt.get("field_relations", {})
    ea: dict = ckpt.get("explore_all", {})


    baseline_cr_yaml: str = ea.get("current_cr_yaml", "") or yaml.dump(seed_cr)
    try:
        baseline_cr: dict = yaml.safe_load(baseline_cr_yaml) or seed_cr
    except Exception:
        baseline_cr = seed_cr


    logger.info("[coverage-test] 收集基准插桩数据...")
    with _timed_step("基准数据收集"):
        baseline_instr, _, baseline_ok, _, _ = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=baseline_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
        )
    if not baseline_ok or baseline_instr is None:
        logger.error("[coverage-test] 基准数据收集失败")
        return []

    baseline_traces = {
        t["branch_index"]: t for t in (baseline_instr.get("traces") or [])
    }
    results: List[dict] = []

    for idx, tgt in enumerate(targets):
        bi: int = tgt["branch_index"]
        target_value: bool = tgt["target_value"]
        bm: dict = branch_meta_index.get(bi, {})
        cond_str: str = bm.get("Fmt") or bm.get("Raw") or ""

        logger.info(
            f"\n[coverage-test {idx + 1}/{len(targets)}] "
            f"branch[{bi}] `{cond_str[:60]}` → {'True' if target_value else 'False'}"
        )

        current_baseline_value: Optional[bool] = None
        if bi in baseline_traces:
            current_baseline_value = baseline_traces[bi].get("value")

        related: List[dict] = _related_fields_for_branch(bi, field_relations)
        src_ctx: str = ""
        if project_path and instrument_dir:
            with _timed_step("源码上下文获取", f"branch[{bi}]"):
                src_ctx = _get_branch_source_context(project_path, instrument_dir, bi)

        success = False
        last_error = ""
        mutated_cr: Optional[dict] = None
        after_instr: Optional[dict] = None
        after_traces: dict = {}
        cr_diff: dict = {}
        attempt_logs: List[dict] = []

        _related_fps = [r["field_path"] for r in related]
        _relevant_constraints = filter_constraints(constraints_data or {}, _related_fps)
        _constraints_txt = format_constraints_section(_relevant_constraints)

        for attempt in range(1, max_retries + 1):
            logger.info(f"  [尝试 {attempt}/{max_retries}]")
            _t_attempt = _time.monotonic()

            prompt = _build_branch_flip_prompt(
                branch_meta=bm,
                current_value=current_baseline_value,
                target_value=target_value,
                source_context=src_ctx,
                related_fields=related,
                base_cr_yaml=baseline_cr_yaml,
                crd_file=crd_file,
                cr_kind=cr_kind,
                error_feedback=last_error,
                include_source_code=include_source_code,
                constraints_txt=_constraints_txt,
            )

            _t_llm = _time.monotonic()
            with _timed_step("LLM 生成 patch", f"branch[{bi}] 尝试{attempt}"):
                action, llm_result = _call_llm_for_branch_flip(prompt)
            _llm_sec = _time.monotonic() - _t_llm

            attempt_record: dict = {
                "attempt": attempt,
                "prompt": prompt,
                "llm_sec": round(_llm_sec, 2),
                "action": action,
                "response": llm_result[:2000] if llm_result else "",
                "apply_sec": None,
                "outcome": "",
            }

            if action == "error" or not llm_result:
                last_error = f"LLM error: {llm_result}"
                attempt_record["outcome"] = f"llm_error: {last_error[:120]}"
                attempt_logs.append(attempt_record)
                logger.warning(f"  LLM 失败: {last_error[:120]}")
                continue


            patch, parse_err = _parse_llm_patch(llm_result)
            if parse_err:
                last_error = f"Patch parse error: {parse_err}. Remember: output ONLY 'set:' and 'delete:' keys."
                attempt_record["outcome"] = f"parse_error: {parse_err}"
                attempt_logs.append(attempt_record)
                logger.warning(f"  Patch 解析失败: {parse_err}")
                continue
            patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
            if crd_err:
                last_error = crd_err
                attempt_record["outcome"] = f"crd_invalid: {crd_err[:120]}"
                attempt_logs.append(attempt_record)
                if not patch.get("set"):
                    continue
            logger.debug(
                f"  Patch: set={list(patch['set'].keys())} delete={patch['delete']}"
            )
            try:
                mutated_cr = _apply_patch_to_cr(baseline_cr, patch)
                mutated_cr.setdefault("metadata", {})["name"] = seed_cr["metadata"][
                    "name"
                ]
                mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
                attempt_record["cr_yaml"] = yaml.dump(mutated_cr)
            except Exception as e:
                last_error = f"Patch apply error: {e}"
                attempt_record["outcome"] = f"patch_apply_error: {e}"
                attempt_logs.append(attempt_record)
                logger.warning(f"  Patch 应用失败: {e}")
                continue

            _t_apply = _time.monotonic()
            with _timed_step("CR apply+collect", f"branch[{bi}] 尝试{attempt}"):
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
            attempt_record["apply_sec"] = round(_time.monotonic() - _t_apply, 2)

            if not ok or after_instr_tmp is None:
                stderr = _cluster_apply._last_create_stderr
                last_error = (
                    stderr.strip() if stderr.strip() else "kubectl apply failed"
                )
                attempt_record["outcome"] = f"apply_failed: {last_error[:120]}"
                attempt_logs.append(attempt_record)
                logger.warning(f"  apply 失败: {last_error[:120]}")
                continue

            after_instr = after_instr_tmp
            after_traces = {
                t["branch_index"]: t for t in (after_instr.get("traces") or [])
            }

            achieved_value: Optional[bool] = None
            if bi in after_traces:
                achieved_value = after_traces[bi].get("value")

            success_flag = achieved_value == target_value
            logger.info(
                f"  achieved={achieved_value}, target={target_value} → "
                f"{'✓ SUCCESS' if success_flag else '✗ MISMATCH'}"
            )

            cr_diff = diff_branch_sequences(baseline_instr, after_instr)
            success = success_flag
            last_error = (
                ""
                if success
                else f"Branch value={achieved_value}, expected={target_value}"
            )
            attempt_record["outcome"] = (
                "success" if success else f"mismatch: achieved={achieved_value}"
            )
            attempt_record["total_sec"] = round(_time.monotonic() - _t_attempt, 2)
            attempt_logs.append(attempt_record)
            break
        else:

            if attempt_logs and attempt_logs[-1].get("total_sec") is None:
                attempt_logs[-1]["total_sec"] = round(_time.monotonic() - _t_attempt, 2)


        cr_changed = _cr_changed_fields(baseline_cr, mutated_cr) if mutated_cr else []


        branch_diff_summary = cr_diff if cr_diff else {}

        results.append(
            {
                "branch_index": bi,
                "target_value": target_value,
                "condition": cond_str,
                "func": bm.get("Func", ""),
                "file": bm.get("File", ""),
                "line": bm.get("Line", ""),
                "call_level": bm.get("CallLevel", "?"),
                "baseline_value": current_baseline_value,
                "achieved_value": (
                    after_traces.get(bi, {}).get("value") if after_instr else None
                ),
                "success": success,
                "error": last_error,
                "baseline_cr_yaml": baseline_cr_yaml,
                "mutated_cr_yaml": yaml.dump(mutated_cr) if mutated_cr else "",
                "cr_changed_fields": cr_changed,
                "related_fields": [r["field_path"] for r in related],
                "branch_diff": {
                    "changed": len(branch_diff_summary.get("changed", [])),
                    "added": len(branch_diff_summary.get("added", [])),
                    "removed": len(branch_diff_summary.get("removed", [])),
                },
                "changed_branches": branch_diff_summary.get("changed", []),
                "attempt_logs": attempt_logs,
            }
        )
        logger.info(
            f"  结果: {'SUCCESS ✓' if success else 'FAILED ✗'}  "
            f"CR字段变化={len(cr_changed)}"
        )

    return results