import logging
import random
from typing import List, Optional

import yaml

import cluster.apply as _cluster_apply
from acto.kubectl_client import KubectlClient
from checkpoint.store import (
    _save_checkpoint,
)
from cluster.apply import apply_cr_and_collect
from core.patch import _apply_patch_to_cr, _parse_llm_patch
from core.timing import _timed_step
from crd.validation import _validate_patch_against_crd
from instrumentation.diff import (
    _extract_branch_values_from_instr,
    diff_branch_sequences,
)
from llm.client import _call_llm_for_branch_flip
from llm.prompts import (
    _build_phase1_prompt,
)
from relations.tracker import (
    _update_field_relations_from_diff,
)

logger = logging.getLogger(__name__)


def run_association_analysis_phase(
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

    num_fields: int = 10,
    max_retries: int = 3,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    branch_meta_index: Optional[dict] = None,
    instrument_prefix: str = "",
):
    """Phase 1: 对随机 CRD 字段进行 LLM 变异，收集数据，建立 field→pred 映射。"""
    logger.info("=" * 70)
    logger.info("Phase 1 — 关联分析")
    logger.info("=" * 70)

    p1 = ckpt["phase1"]
    p2 = ckpt["phase2"]
    field_relations = ckpt["field_relations"]
    completed = set(p1["completed_fields"])
    declared_field_paths: set = {f["path"] for f in crd_fields}


    available = [f for f in crd_fields if f["path"] not in completed]
    random.shuffle(available)
    to_do = available[: max(0, num_fields - len(completed))]

    if not to_do:
        logger.info(f"Phase 1 已完成 {len(completed)} 个字段，无新任务")
        return

    logger.info(
        f"Phase 1: 已完成 {len(completed)}/{num_fields}, 本次新增 {len(to_do)} 个字段"
    )

    seed_cr_yaml = yaml.dump(seed_cr)

    logger.info("[Phase1] 收集基准插桩数据...")
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
        logger.error("[Phase1] 基准数据收集失败")
        return


    baseline_values = _extract_branch_values_from_instr(baseline_instr)
    for bi, v in baseline_values.items():
        if v is not None:
            p2["coverage_map"].setdefault(str(bi), [])
            v_str = str(v)
            if v_str not in p2["coverage_map"][str(bi)]:
                p2["coverage_map"][str(bi)].append(v_str)

    for idx, field_info in enumerate(to_do):
        field_path = field_info["path"]
        logger.info(f"\n[Phase1 {idx + 1}/{len(to_do)}] 变异字段: {field_path}")

        error_feedback = ""
        success = False
        last_error = ""
        last_attempted_cr_yaml = ""

        for attempt_n in range(1, max_retries + 1):
            logger.info(f"  [尝试 {attempt_n}/{max_retries}]")


            prompt = _build_phase1_prompt(
                base_cr_yaml=seed_cr_yaml,
                field_path=field_path,
                crd_file=crd_file,
                cr_kind=cr_kind,
                error_feedback=error_feedback,
                base_cr=seed_cr,
            )

            with _timed_step("LLM 变异", field_path):
                action, new_cr_yaml = _call_llm_for_branch_flip(prompt)

            if action == "error" or not new_cr_yaml:
                logger.warning(
                    f"  LLM 变异失败: {new_cr_yaml[:200] if new_cr_yaml else ''}"
                )
                error_feedback = f"LLM error: {new_cr_yaml}"
                last_error = error_feedback
                continue


            patch, parse_err = _parse_llm_patch(new_cr_yaml)
            if parse_err:
                logger.warning(f"  Patch 解析失败: {parse_err}")
                error_feedback = f"Patch parse error: {parse_err}. Remember: output ONLY 'set:' and 'delete:' keys."
                last_error = error_feedback
                continue
            patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
            if crd_err:
                error_feedback = crd_err
                last_error = error_feedback
                if not patch.get("set"):
                    continue
            logger.debug(
                f"  Patch: set={list(patch['set'].keys())} delete={patch['delete']}"
            )
            try:
                mutated_cr = _apply_patch_to_cr(seed_cr, patch)
                mutated_cr.setdefault("metadata", {})["name"] = seed_cr["metadata"][
                    "name"
                ]
                mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
                last_attempted_cr_yaml = yaml.dump(mutated_cr)
            except Exception as e:
                logger.warning(f"  Patch 应用失败: {e}")
                error_feedback = f"Patch apply error: {e}"
                last_error = error_feedback
                continue


            after_instr, _, ok, _, _ = apply_cr_and_collect(
                kubectl_client=kubectl_client,
                namespace=namespace,
                cluster_name=cluster_name,
                input_cr=mutated_cr,
                operator_container_name=operator_container_name,
                wait_sec=wait_sec,
                collect_max_wait=collect_max_wait,
                instrument_prefix=instrument_prefix,
            )

            if not ok or after_instr is None:

                stderr = _cluster_apply._last_create_stderr
                error_feedback = (
                    stderr.strip() if stderr.strip() else "kubectl create failed"
                )
                logger.warning(
                    f"  apply/collect 失败，下轮 feedback: {error_feedback[:200]}"
                )
                last_error = error_feedback
                continue


            diff = diff_branch_sequences(baseline_instr, after_instr)
            n_changed = len(diff.get("changed", []))
            n_added = len(diff.get("added", []))
            n_removed = len(diff.get("removed", []))
            logger.info(
                f"  ✓ 成功 diff: changed={n_changed}, added={n_added}, removed={n_removed}"
            )

            _update_field_relations_from_diff(
                field_relations=field_relations,
                diff=diff,
                cr_before=seed_cr,
                cr_after=mutated_cr,
                mutation_round=f"phase1-{field_path}",
                branch_meta_index=branch_meta_index,
                declared_field_paths=declared_field_paths,
            )


            after_values = _extract_branch_values_from_instr(after_instr)
            for bi, v in after_values.items():
                if v is not None:
                    p2["coverage_map"].setdefault(str(bi), [])
                    v_str = str(v)
                    if v_str not in p2["coverage_map"][str(bi)]:
                        p2["coverage_map"][str(bi)].append(v_str)

            success = True
            break

        p1["completed_fields"].append(field_path)
        p1["mutation_log"].append(
            {
                "field": field_path,
                "status": "ok" if success else "failed",
                "error": "" if success else last_error,
                "last_attempted_cr_yaml": "" if success else last_attempted_cr_yaml,
            }
        )
        _save_checkpoint(ckpt_path, ckpt)

    logger.info(
        f"\nPhase 1 完成: {len(p1['completed_fields'])} 个字段已分析, "
        f"field_relations 涵盖 {len(field_relations)} 个字段"
    )