

import json as _json
import logging
import os
from typing import Optional

import yaml

import cluster.apply as _cluster_apply
from checkpoint.store import _load_checkpoint, _save_checkpoint
from cluster.apply import apply_cr_and_collect
from core.patch import _apply_patch_to_cr, _parse_llm_patch
from core.timing import _timed_step
from crd.schema import _extract_crd_required_fields, get_crd_file_path
from crd.validation import _repair_required_fields, _validate_patch_against_crd
from instrumentation.diff import _build_branch_index, _extract_branch_values_from_instr
from instrumentation.source import _get_branch_source_context
from llm.client import _call_llm_for_branch_flip
from llm.constraints import (
    ensure_constraints,
    filter_constraints,
    format_constraints_section,
)
from llm.prompts import _build_branch_flip_prompt, _related_fields_for_branch
from runner.common import (
    init_cluster_env,
    load_base_cr,
    load_gsod_context,
    load_operator_config,
    setup_runner_workdir,
    teardown_cluster,
)

logger = logging.getLogger(__name__)


def _parse_target_key(target_key: str) -> tuple:
    """'42_T' -> (42, True),  '42_F' -> (42, False)."""
    parts = target_key.split("_")
    if len(parts) != 2 or parts[1] not in ("T", "F"):
        raise ValueError(
            f"无效 target_key 格式: {target_key!r}  (期望 '<branch_index>_T' 或 '<branch_index>_F')"
        )
    return int(parts[0]), parts[1] == "T"


def _branches_in_instr(instr: dict) -> set:
    return {t["branch_index"] for t in instr.get("traces", []) if "branch_index" in t}


def run_testplan_probe(
    checkpoint_path: str,
    config_path: str,
    instrument_info_path: str,
    testcase_id: str,
    target_key: str,
    context_file: str = "",
    field_relations_path: str = "",
    project_path: str = "",
    instrument_dir: str = "",
    include_source_code: bool = False,
    max_retries: int = 3,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    workdir_base: str = "gsod_output_v5",
    keep_cluster: bool = False,
    reuse_cluster_name: str = "",
    base_cr_path: str = "",
    no_llm: bool = False,
    debug: bool = False,
    operator_image: str = "",
    cr_kind: str = "",
    instrument_prefix: str = "",
) -> None:
    """Run a single testplan probe and print full diagnostics."""
    if debug:
        logger.setLevel(logging.DEBUG)


    if not os.path.exists(checkpoint_path):
        raise FileNotFoundError(f"Checkpoint 文件不存在: {checkpoint_path}")
    ckpt = _load_checkpoint(checkpoint_path)
    tp = ckpt.get("testplan", {})

    testcases: dict = tp.get("testcases", {})
    targets: dict = tp.get("targets", {})
    coverage_map: dict = tp.get("coverage_map", {})

    if testcase_id not in testcases:
        available = sorted(testcases.keys(), key=lambda x: int(x) if x.isdigit() else x)
        raise KeyError(
            f"Testcase ID '{testcase_id}' 不存在于 checkpoint。\n"
            f"可用 ID: {available[:30]}{'...' if len(available) > 30 else ''}"
        )
    tc = testcases[testcase_id]

    if target_key not in targets:
        available_tkeys = sorted(targets.keys())
        raise KeyError(
            f"Target key '{target_key}' 不存在于 checkpoint。\n"
            f"可用 key 示例: {available_tkeys[:30]}{'...' if len(available_tkeys) > 30 else ''}"
        )
    tgt_meta = targets[target_key]
    bi, want_value = _parse_target_key(target_key)


    field_relations: dict = ckpt.get("field_relations") or {}
    if (
        not field_relations
        and field_relations_path
        and os.path.exists(field_relations_path)
    ):
        with open(field_relations_path, "r", encoding="utf-8") as _f:
            field_relations = _json.load(_f)


    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "probe", operator_name)

    config = load_operator_config(config_path)
    branch_meta_index = _build_branch_index(instrument_info_path)
    crd_file = get_crd_file_path(config, config_dir) or ""


    _profile_dir = (
        os.path.dirname(os.path.abspath(context_file)) if context_file else config_dir
    )
    constraints_data = (
        ensure_constraints(context_file, _profile_dir) if context_file else {}
    )

    gsod_context = load_gsod_context(context_file)
    env = init_cluster_env(
        config,
        config_dir,
        gsod_context,
        workdir,
        "gsod-probe",
        reuse_cluster_name,
        operator_image=operator_image,
    )
    if env is None:
        raise RuntimeError("集群初始化失败")

    cluster_name = env["cluster_name"]
    kubectl_client = env["kubectl_client"]
    namespace = env["namespace"]
    operator_container_name = env["operator_container_name"]
    seed_cr = env["seed_cr"]
    cr_kind = cr_kind or seed_cr.get("kind", "")

    if base_cr_path:
        new_cr = load_base_cr(base_cr_path, seed_cr, namespace, cr_kind, strict=True)
        if new_cr is not None:
            seed_cr = new_cr
            cr_kind = seed_cr.get("kind", cr_kind)


    bm = branch_meta_index.get(bi, {})
    print("\n" + "=" * 70)
    print("  GSOD TestPlan Probe")
    print(
        f"  testcase_id : {testcase_id}  (frequency={tc.get('frequency', 0)}, "
        f"has_new_branch={tc.get('has_new_branch', '?')})"
    )
    print(
        f"  target_key  : {target_key}  ({'ALREADY RESOLVED' if tgt_meta.get('resolved') else 'open'})"
    )
    print(f"  branch[{bi}]  : {bm.get('Fmt') or bm.get('Raw', '(no meta)')}")
    print(f"  want_value  : {'True' if want_value else 'False'}")
    print(f"  crd_file    : {crd_file or '(none)'}")
    print("=" * 70)


    llm_prompt: str = ""
    llm_raw: str = ""
    llm_patch: dict = {}
    patch_errors: list = []
    base_cr_yaml: str = tc.get("cr", "")
    mutated_cr: dict = yaml.safe_load(base_cr_yaml) or seed_cr


    instr: Optional[dict] = None
    ok: bool = False

    if no_llm:
        print("\n[probe] --no-llm: 直接使用 testcase 原始 CR 进行 apply")
        print("\n[probe] 正在 apply CR 并收集插桩数据...")
        instr, _, ok, _, _ = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=mutated_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
        )
        if not ok or instr is None:
            print("[probe] apply/collect 失败，无法生成覆盖报告")
            return
    else:
        _skip_reset = False
        related = _related_fields_for_branch(bi, field_relations)
        src_ctx = ""
        if project_path and instrument_dir:
            src_ctx = _get_branch_source_context(project_path, instrument_dir, bi)

        required_fields = _extract_crd_required_fields(crd_file, cr_kind)

        _related_fps = [r["field_path"] for r in related]
        _relevant_constraints = filter_constraints(constraints_data, _related_fps)
        _constraints_txt = format_constraints_section(_relevant_constraints)

        error_feedback = ""
        for attempt_n in range(1, max_retries + 1):
            print(f"\n[probe] LLM attempt {attempt_n}/{max_retries} ...")
            llm_prompt = _build_branch_flip_prompt(
                branch_meta=bm,
                current_value=None,
                target_value=want_value,
                source_context=src_ctx,
                related_fields=related,
                base_cr_yaml=base_cr_yaml,
                crd_file=crd_file,
                cr_kind=cr_kind,
                error_feedback=error_feedback,
                include_source_code=include_source_code,
                constraints_txt=_constraints_txt,
            )
            with _timed_step("LLM branch-flip", f"b[{bi}]"):
                action, llm_raw = _call_llm_for_branch_flip(llm_prompt)

            print(f"\n--- LLM raw output (attempt {attempt_n}) ---")
            print(llm_raw)
            print(f"Error feedback: {error_feedback}")
            print("---")

            if action == "error":
                err = f"LLM error: {llm_raw[:300]}"
                patch_errors.append(err)
                error_feedback += "\n" + err
                continue

            patch, parse_err = _parse_llm_patch(llm_raw)
            if parse_err:
                err = f"Patch parse error: {parse_err}"
                patch_errors.append(err)
                error_feedback += "\n" + err
                continue

            patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
            if crd_err:
                patch_errors.append(f"CRD validation: {crd_err}")
                if not patch.get("set"):
                    error_feedback += "\n" + crd_err
                    continue

            try:
                base_cr_obj = yaml.safe_load(base_cr_yaml) or seed_cr
                mutated_cr = _apply_patch_to_cr(base_cr_obj, patch)
                mutated_cr.setdefault("metadata", {})["name"] = seed_cr["metadata"][
                    "name"
                ]
                mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
                if required_fields:
                    mutated_cr, repaired = _repair_required_fields(
                        mutated_cr, base_cr_obj, required_fields, f"spec.branch{bi}"
                    )
                    if repaired:
                        print(f"  [probe] 自动补回必填字段: {repaired}")
                llm_patch = patch
            except Exception as e:
                err = f"Patch apply error: {e}"
                patch_errors.append(err)
                error_feedback += "\n" + err
                continue


            print("\n[probe] 正在 apply CR 并收集插桩数据...")
            instr, _, ok, _is_rejection, _ = apply_cr_and_collect(
                kubectl_client=kubectl_client,
                namespace=namespace,
                cluster_name=cluster_name,
                input_cr=mutated_cr,
                operator_container_name=operator_container_name,
                wait_sec=wait_sec,
                collect_max_wait=collect_max_wait,
                skip_cluster_reset=_skip_reset,
                instrument_prefix=instrument_prefix,
            )
            _skip_reset = _is_rejection
            if ok and instr is not None:
                break


            apply_stderr = _cluster_apply._last_create_stderr
            err = f"kubectl create failed: {apply_stderr[:600]}"
            patch_errors.append(err)
            error_feedback += "\n" + err
            print(
                f"[probe] apply 失败，错误已反馈给下一次 LLM 重试: {apply_stderr[:200]}"
            )
        else:
            print(
                f"\n[probe] 所有 {max_retries} 次尝试均失败（含 apply 错误反馈），无法生成覆盖报告"
            )
            return

        if not ok or instr is None:
            print("[probe] apply/collect 失败，无法生成覆盖报告")
            return

    try:

        branch_values = _extract_branch_values_from_instr(instr)
        covered_branches = _branches_in_instr(instr)

        prev_covered = {int(k) for k, v in coverage_map.items() if v}
        new_branches = covered_branches - prev_covered

        prev_resolved = {k for k, v in targets.items() if v.get("resolved")}
        newly_resolved = set()
        for tkey, tgt in targets.items():
            if tkey in prev_resolved:
                continue
            parts = tkey.split("_")
            if len(parts) != 2:
                continue
            try:
                t_bi, t_want = int(parts[0]), parts[1] == "T"
            except ValueError:
                continue
            actual = branch_values.get(t_bi)
            if actual is not None and actual == t_want:
                newly_resolved.add(tkey)

        target_branch_value = branch_values.get(bi)


        print("\n" + "=" * 70)
        print("  PROBE RESULTS")
        print("=" * 70)

        if llm_patch:
            print("\n[LLM Patch Applied]")
            print(yaml.dump(llm_patch, default_flow_style=False).rstrip())
        elif not no_llm:
            print("\n[LLM Patch] 未生成有效 patch（使用原始 CR）")

        if patch_errors:
            print("\n[Patch Errors across attempts]")
            for i, e in enumerate(patch_errors, 1):
                print(f"  {i}. {e}")

        print("\n[Branch Coverage]")
        print(f"  本轮覆盖分支数 : {len(covered_branches)}")
        print(f"  新增分支数     : {len(new_branches)}")
        if new_branches:
            new_with_meta = []
            for b in sorted(new_branches):
                m = branch_meta_index.get(b, {})
                cond = (m.get("Fmt") or m.get("Raw") or "")[:60]
                new_with_meta.append(f"    b[{b}] {cond}")
            print("\n".join(new_with_meta))

        print("\n[Target Resolution]")
        print(f"  本轮已解决目标数 : {len(newly_resolved)}")
        if newly_resolved:
            print(f"  新解决目标       : {sorted(newly_resolved)}")

        print("\n[Target Branch Value]")
        print(
            f"  target_key : {target_key}  (branch[{bi}] want={'T' if want_value else 'F'})"
        )
        if target_branch_value is None:
            print("  实际取值   : (未被 trace — 该分支在本轮未执行到)")
        else:
            hit = target_branch_value == want_value
            print(
                f"  实际取值   : {'True' if target_branch_value else 'False'}  "
                f"{'✓ HIT' if hit else '✗ MISS'}"
            )

        print("\n[Checkpoint Target Status]")
        print(f"  was_resolved : {tgt_meta.get('resolved', False)}")
        print(f"  testcase_ids : {tgt_meta.get('testcase_id', [])}")


        hit = target_branch_value is not None and target_branch_value == want_value
        tgt_entry = targets.get(target_key)
        if hit and tgt_entry and not tgt_entry.get("resolved"):
            tgt_entry["probe_pending"] = True
            tp = ckpt.get("testplan", {})
            tp["targets"] = targets
            _save_checkpoint(checkpoint_path, ckpt)
            print(
                f"\n[probe] 已在 checkpoint 中为 {target_key} 打上 probe_pending 标记"
            )
        else:
            reason = (
                "已解决"
                if tgt_entry and tgt_entry.get("resolved")
                else "MISS（未命中目标值）"
                if not hit
                else "不存在"
            )
            print(f"\n[probe] 未打标记: {target_key}（{reason}）")

        print("\n" + "=" * 70)

    finally:
        teardown_cluster(env, keep_cluster, reuse_cluster_name)