import logging
from itertools import combinations as _combinations
from typing import Dict, List, Optional

import yaml

import cluster.apply as _cluster_apply
from acto.kubectl_client import KubectlClient
from checkpoint.store import (
    _load_branch_baseline_crs,
    _save_checkpoint,
    _update_branch_baseline_crs,
)
from cluster.apply import apply_cr_and_collect
from core.patch import _apply_patch_to_cr, _parse_llm_patch
from core.rich_logger import update_progress, update_status
from core.timing import _timed_step
from crd.validation import _validate_patch_against_crd
from instrumentation.diff import (
    _extract_branch_values_from_instr,
    diff_branch_sequences,
)
from instrumentation.source import _get_branch_source_context
from llm.client import _call_llm_for_branch_flip
from llm.prompts import (
    _build_branch_flip_prompt,
    _get_baseline_cr_for_branch,
    _related_fields_for_branch,
)
from relations.tracker import (
    _update_field_relations_from_diff,
)

logger = logging.getLogger(__name__)


def make_target_key(tgt: tuple) -> str:
    """将目标元组转换为字符串键，如 '42:T|43:F'。"""
    return "|".join(f"{bi}:{'T' if v else 'F'}" for bi, v in sorted(tgt))


def is_target_covered(tgt: tuple, coverage_map: Dict[int, set]) -> bool:
    """检查目标元组中所有 (branch_index, value) 是否已被覆盖。"""
    return all(v in coverage_map.get(bi, set()) for bi, v in tgt)


def find_newly_covered_targets(
    all_targets: List[tuple],
    test_plan: Dict[str, dict],
    new_values: Dict[int, Optional[bool]],
) -> List[tuple]:
    """返回本次 apply 新覆盖但尚未登记到 test_plan 的目标列表。"""
    return [
        tgt
        for tgt in all_targets
        if make_target_key(tgt) not in test_plan
        and all(new_values.get(bi) == v for bi, v in tgt)
    ]


def get_sorted_uncovered_targets(
    all_targets: List[tuple],
    coverage_map: Dict[int, set],
    attempted_keys: set,
    has_relation: set,
    branch_meta_index: dict,
) -> List[tuple]:
    """返回当前仍未覆盖且未尝试过的目标，按优先级排序：
    1. 有 field_relations 映射的 branch 优先（已知关联更容易被翻转）
    2. CallLevel 越深越优先（深层分支更接近业务逻辑）
    3. BranchIndex 升序（稳定排序）
    """
    remaining = [
        tgt
        for tgt in all_targets
        if not is_target_covered(tgt, coverage_map)
        and make_target_key(tgt) not in attempted_keys
    ]
    remaining.sort(
        key=lambda tgt: (
            0 if any(bi in has_relation for bi, _ in tgt) else 1,
            -max(branch_meta_index.get(bi, {}).get("CallLevel", 0) for bi, _ in tgt),
            min(bi for bi, _ in tgt),
        )
    )
    return remaining


def _restore_phase2_coverage_map(
    p2: dict,
    all_branch_indices: List[int],
) -> Dict[int, set]:
    """从 checkpoint 恢复 coverage_map，所有已知 branch 初始化为空集合。"""
    coverage_map: Dict[int, set] = {bi: set() for bi in all_branch_indices}
    for bi_str, vals in p2.get("coverage_map", {}).items():
        bi_int = int(bi_str)
        if bi_int in coverage_map:
            for v in vals:
                if v in ("True", "true"):
                    coverage_map[bi_int].add(True)
                elif v in ("False", "false"):
                    coverage_map[bi_int].add(False)
    return coverage_map


def _collect_phase2_seed_baseline(
    p2: dict,
    ckpt: dict,
    ckpt_path: str,
    kubectl_client,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    seed_cr: dict,
    wait_sec: int,
    collect_max_wait: int,
    instrument_prefix: str = "",
) -> Optional[dict]:
    """Step 2: 收集或从 checkpoint 恢复 seed CR 基准插桩数据。

    返回 baseline_instr，失败则返回 None。
    """
    baseline_instr = p2.get("baseline_instr")
    if baseline_instr is None:
        logger.info("[Step 2] 收集 seed CR 基准插桩数据...")
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
            logger.error("[Step 2] 基准数据收集失败，终止 Phase 2")
            return None
        p2["baseline_instr"] = baseline_instr
        _save_checkpoint(ckpt_path, ckpt)
    else:
        logger.info("[Step 2] 使用 checkpoint 中的基准数据")
    return baseline_instr


def _register_seed_baseline_targets(
    all_targets: List[tuple],
    baseline_values: Dict[int, Optional[bool]],
    coverage_map: Dict[int, set],
    test_plan: Dict[str, dict],
    seed_cr_yaml: str,
) -> List[tuple]:
    """将 seed CR 已覆盖的目标登记到 test_plan（source=baseline）。

    同时更新 coverage_map，返回新登记的目标列表。
    """
    for bi, v in baseline_values.items():
        if v is not None:
            coverage_map.setdefault(bi, set()).add(v)
    newly_covered = find_newly_covered_targets(all_targets, test_plan, baseline_values)
    for tgt in newly_covered:
        tk = make_target_key(tgt)
        test_plan[tk] = {
            "target_key": tk,
            "targets": [{"branch_index": bi, "target_value": v} for bi, v in tgt],
            "cr_yaml": seed_cr_yaml,
            "source": "baseline",
            "attempt": 0,
        }
    return newly_covered


def _process_coverage_target(
    tgt: tuple,
    mutation_round: int,
    branch_meta_index: dict,
    field_relations: dict,
    seed_cr: dict,
    seed_cr_yaml: str,
    namespace: str,
    baseline_instr: dict,
    baseline_values: Dict[int, Optional[bool]],
    all_targets: List[tuple],
    coverage_map: Dict[int, set],
    test_plan: Dict[str, dict],
    has_relation: set,
    branch_baseline_crs: dict,
    kubectl_client,
    cluster_name: str,
    operator_container_name: str,
    crd_file: str,
    cr_kind: str,
    config_path: str,
    max_retries: int,
    wait_sec: int,
    collect_max_wait: int,
    project_path: str,
    instrument_dir: str,
    include_source_code: bool,
    instrument_prefix: str = "",
) -> dict:
    """处理单个覆盖目标：LLM → patch → apply 重试循环。

    原地修改 coverage_map、test_plan、has_relation、branch_baseline_crs。
    返回 log_entry dict（由调用方追加到 explore_log）。
    """
    tk = make_target_key(tgt)
    primary_bi, primary_tv = tgt[0]
    bm_primary = branch_meta_index.get(primary_bi, {})
    related = _related_fields_for_branch(primary_bi, field_relations)

    logger.info(f"\n--- [Step 3 轮次 {mutation_round}] 目标: {tk} ---")
    for bi, tv in tgt:
        bm = branch_meta_index.get(bi, {})
        cond = (bm.get("Fmt") or bm.get("Raw") or "?")[:60]
        level = bm.get("CallLevel", "?")
        logger.info(
            f"  branch[{bi}] level={level} `{cond}` → {'True' if tv else 'False'}"
        )

    src_ctx = ""
    if project_path and instrument_dir:
        src_ctx = _get_branch_source_context(project_path, instrument_dir, primary_bi)

    combo_extra = (
        [
            {"branch_meta": branch_meta_index.get(bi, {}), "target_value": tv}
            for bi, tv in tgt[1:]
        ]
        if len(tgt) > 1
        else []
    )

    log_entry: dict = {
        "round": mutation_round,
        "target_key": tk,
        "targets": [{"branch_index": bi, "target_value": tv} for bi, tv in tgt],
        "attempts": [],
        "success": False,
        "side_covered": [],
    }

    error_feedback = ""
    success = False

    for attempt_n in range(1, max_retries + 1):
        logger.info(f"  [尝试 {attempt_n}/{max_retries}]")

        branch_base_cr_yaml = _get_baseline_cr_for_branch(
            branch_baseline_crs, primary_bi, seed_cr_yaml
        )
        if branch_base_cr_yaml != seed_cr_yaml:
            logger.info(
                f"  使用 branch_baseline_crs 中 branch[{primary_bi}] 对应的 CR 作为变异起点"
            )

        prompt = _build_branch_flip_prompt(
            branch_meta=bm_primary,
            current_value=baseline_values.get(primary_bi),
            target_value=primary_tv,
            source_context=src_ctx,
            related_fields=related,
            base_cr_yaml=branch_base_cr_yaml,
            crd_file=crd_file,
            cr_kind=cr_kind,
            combo_targets=combo_extra,
            error_feedback=error_feedback,
            include_source_code=include_source_code,
        )

        with _timed_step("LLM branch-flip", f"branch[{primary_bi}]"):
            action, llm_result = _call_llm_for_branch_flip(prompt)

        attempt_rec: dict = {
            "n": attempt_n,
            "action": action,
            "cr_yaml": None,
            "flip_success": False,
            "error": None,
            "branch_values_after": {},
        }

        if action == "error":
            logger.warning(f"  LLM 错误: {llm_result[:200]}")
            attempt_rec["error"] = llm_result
            error_feedback = f"LLM error: {llm_result}"
            log_entry["attempts"].append(attempt_rec)
            continue

        patch, parse_err = _parse_llm_patch(llm_result)
        if parse_err:
            logger.warning(f"  Patch 解析失败: {parse_err}")
            attempt_rec["error"] = f"Patch parse: {parse_err}"
            error_feedback = f"Patch parse error: {parse_err}. Remember: output ONLY 'set:' and 'delete:' keys."
            log_entry["attempts"].append(attempt_rec)
            continue

        patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
        if crd_err:
            attempt_rec["error"] = f"CRD validation: {crd_err[:120]}"
            error_feedback = crd_err
            log_entry["attempts"].append(attempt_rec)
            if not patch.get("set"):
                continue
        logger.debug(
            f"  Patch: set={list(patch['set'].keys())} delete={patch['delete']}"
        )
        try:
            _base_for_patch = yaml.safe_load(branch_base_cr_yaml) or seed_cr
            mutated_cr = _apply_patch_to_cr(_base_for_patch, patch)
            mutated_cr.setdefault("metadata", {})["name"] = seed_cr["metadata"]["name"]
            mutated_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
        except Exception as e:
            logger.warning(f"  Patch 应用失败: {e}")
            attempt_rec["error"] = f"Patch apply: {e}"
            error_feedback = f"Patch apply error: {e}"
            log_entry["attempts"].append(attempt_rec)
            continue

        attempt_rec["cr_yaml"] = yaml.dump(mutated_cr)

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
            stderr = _cluster_apply._last_create_stderr
            _fb = (
                stderr.strip()
                if stderr.strip()
                else "kubectl create failed – generate a VALID CR"
            )
            logger.warning(f"  apply/collect 失败: {_fb[:200]}")
            attempt_rec["error"] = _fb
            error_feedback = _fb
            log_entry["attempts"].append(attempt_rec)
            continue

        new_values: Dict[int, Optional[bool]] = _extract_branch_values_from_instr(instr)
        attempt_rec["branch_values_after"] = {
            str(bi): str(v) for bi, v in new_values.items()
        }

        for bi_v, v in new_values.items():
            if v is not None:
                coverage_map.setdefault(bi_v, set()).add(v)

        diff = diff_branch_sequences(baseline_instr, instr)
        try:
            _branch_base_cr_before = yaml.safe_load(branch_base_cr_yaml)
        except Exception:
            _branch_base_cr_before = seed_cr
        _update_field_relations_from_diff(
            field_relations=field_relations,
            diff=diff,
            cr_before=_branch_base_cr_before,
            cr_after=mutated_cr,
            mutation_round=f"phase2-r{mutation_round}-a{attempt_n}",
            branch_meta_index=branch_meta_index,
            declared_field_paths=set(field_relations.keys()) or None,
        )
        for fdata in field_relations.values():
            for bi in fdata.get("branch_indices") or []:
                has_relation.add(bi)

        side_covered = find_newly_covered_targets(all_targets, test_plan, new_values)
        for side_tgt in side_covered:
            side_tk = make_target_key(side_tgt)
            test_plan[side_tk] = {
                "target_key": side_tk,
                "targets": [
                    {"branch_index": bi, "target_value": v} for bi, v in side_tgt
                ],
                "cr_yaml": yaml.dump(mutated_cr),
                "source": "mutation",
                "mutation_round": mutation_round,
                "attempt": attempt_n,
            }
        log_entry["side_covered"] = [make_target_key(t) for t in side_covered]

        primary_hit = all(new_values.get(bi) == tv for bi, tv in tgt)
        attempt_rec["flip_success"] = primary_hit
        log_entry["attempts"].append(attempt_rec)

        if config_path:
            mutated_cr_yaml_p2 = yaml.dump(mutated_cr)
            _new_bbc = _update_branch_baseline_crs(
                config_path, instr, mutated_cr_yaml_p2
            )
            branch_baseline_crs.clear()
            branch_baseline_crs.update(_new_bbc)

        if primary_hit:
            logger.info(f"  ✓ 主目标命中! 额外覆盖 {len(side_covered)} 个目标")
            success = True
            log_entry["success"] = True
            break
        else:
            missed = [
                f"branch[{bi}] got {new_values.get(bi)} want {'T' if tv else 'F'}"
                for bi, tv in tgt
                if new_values.get(bi) != tv
            ]
            feedback = "Branch flip not achieved: " + ", ".join(missed)
            logger.info(f"  ✗ 未命中: {feedback}")
            if side_covered:
                logger.info(f"    但顺带覆盖了 {len(side_covered)} 个其他目标")
            error_feedback = feedback

    if not success:
        logger.info(f"  目标 {tk} 在 {max_retries} 次尝试后未命中，标记为已尝试")

    return log_entry


def run_coverage_generation_phase(
    ckpt: dict,
    ckpt_path: str,

    kubectl_client: KubectlClient,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    seed_cr: dict,
    crd_file: str,
    cr_kind: str,
    branch_meta_index: dict,
    all_branch_indices: List[int],

    k: int = 1,
    max_retries: int = 3,
    max_combos: int = 0,
    project_path: str = "",
    instrument_dir: str = "",
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    config_path: str = "",
    include_source_code: bool = False,
    instrument_prefix: str = "",
):
    """
    Phase 2: k-branch 覆盖测试计划生成。

    流程:
      Step 1 — 计算全量覆盖目标 (branch × 取值 的完整矩阵)
      Step 2 — 收集 seed CR 基准插桩数据，将已覆盖目标登记进测试计划
      Step 3 — 变异循环: 对每个尚未覆盖的目标，让 LLM 生成能翻转该
               branch 取值的 CR，apply 后更新 coverage_map 并同步
               将本次 apply 覆盖到的所有新目标一并登记进测试计划
    """
    logger.info("=" * 70)
    logger.info("Phase 2 — 测试计划生成 (k-branch 覆盖探索)")
    logger.info("=" * 70)
    update_status(phase="Phase 2", current_op="初始化")

    p2 = ckpt["phase2"]
    field_relations = ckpt["field_relations"]
    seed_cr_yaml = yaml.dump(seed_cr)

    branch_baseline_crs: dict = (
        _load_branch_baseline_crs(config_path) if config_path else {}
    )
    if branch_baseline_crs:
        logger.info(
            f"[Phase2] branch_baseline_crs 已加载: {len(branch_baseline_crs)} 条"
        )


    coverage_map = _restore_phase2_coverage_map(p2, all_branch_indices)
    test_plan: Dict[str, dict] = {e["target_key"]: e for e in p2.get("test_plan", [])}
    explore_log: List[dict] = p2.get("explore_log", [])
    attempted_keys: set = set(p2.get("attempted_keys", []))


    all_targets = build_coverage_target_matrix(all_branch_indices, k)
    total_targets = len(all_targets)
    logger.info(
        f"[Step 1] k={k}, 总覆盖目标数: {total_targets} "
        f"({len(all_branch_indices)} branch × {'2' if k == 1 else '2^' + str(k)} 取值)"
    )


    baseline_instr = _collect_phase2_seed_baseline(
        p2=p2,
        ckpt=ckpt,
        ckpt_path=ckpt_path,
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        operator_container_name=operator_container_name,
        seed_cr=seed_cr,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        instrument_prefix=instrument_prefix,
    )
    if baseline_instr is None:
        return

    baseline_values: Dict[int, Optional[bool]] = _extract_branch_values_from_instr(
        baseline_instr
    )
    baseline_covered = _register_seed_baseline_targets(
        all_targets, baseline_values, coverage_map, test_plan, seed_cr_yaml
    )
    already_covered = sum(
        1 for tgt in all_targets if is_target_covered(tgt, coverage_map)
    )
    logger.info(
        f"[Step 2] seed CR 覆盖: {len(baseline_covered)} 个新目标, "
        f"合计已覆盖 {already_covered}/{total_targets}"
    )


    logger.info("[Step 3] 开始变异循环，目标: 覆盖所有未达成的测试目标")
    has_relation: set = {
        bi
        for fdata in field_relations.values()
        for bi in (fdata.get("branch_indices") or [])
    }

    mutation_round = 0
    while True:
        uncovered = get_sorted_uncovered_targets(
            all_targets, coverage_map, attempted_keys, has_relation, branch_meta_index
        )
        if not uncovered:
            logger.info("[Step 3] 所有可探索目标已覆盖，变异循环结束")
            break
        if max_combos > 0 and mutation_round >= max_combos:
            logger.info(f"[Step 3] 已达 max_combos={max_combos} 限制，停止")
            break

        tgt = uncovered[0]
        mutation_round += 1
        _n_covered = sum(1 for t in all_targets if is_target_covered(t, coverage_map))
        update_progress(
            done=_n_covered,
            total=total_targets,
            label="覆盖目标",
            branches_covered=_n_covered,
            branches_total=total_targets,
        )
        update_status(
            current_op=f"b[{tgt['branch_index']}]→{'T' if tgt['target_value'] else 'F'}"
        )

        log_entry = _process_coverage_target(
            tgt=tgt,
            mutation_round=mutation_round,
            branch_meta_index=branch_meta_index,
            field_relations=field_relations,
            seed_cr=seed_cr,
            seed_cr_yaml=seed_cr_yaml,
            namespace=namespace,
            baseline_instr=baseline_instr,
            baseline_values=baseline_values,
            all_targets=all_targets,
            coverage_map=coverage_map,
            test_plan=test_plan,
            has_relation=has_relation,
            branch_baseline_crs=branch_baseline_crs,
            kubectl_client=kubectl_client,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            crd_file=crd_file,
            cr_kind=cr_kind,
            config_path=config_path,
            max_retries=max_retries,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            project_path=project_path,
            instrument_dir=instrument_dir,
            include_source_code=include_source_code,
            instrument_prefix=instrument_prefix,
        )

        attempted_keys.add(make_target_key(tgt))
        explore_log.append(log_entry)

        p2["coverage_map"] = {
            str(bi): sorted(str(v) for v in s) for bi, s in coverage_map.items()
        }
        p2["test_plan"] = list(test_plan.values())
        p2["explore_log"] = explore_log
        p2["attempted_keys"] = sorted(attempted_keys)
        _save_checkpoint(ckpt_path, ckpt)


    covered_targets = sum(
        1 for tgt in all_targets if is_target_covered(tgt, coverage_map)
    )
    bi_both = sum(
        1
        for bi in all_branch_indices
        if True in coverage_map.get(bi, set()) and False in coverage_map.get(bi, set())
    )
    logger.info("\nPhase 2 完成:")
    logger.info(
        f"  覆盖目标: {covered_targets}/{total_targets} "
        f"({100 * covered_targets // total_targets if total_targets else 0}%)"
    )
    logger.info(f"  branch 双面覆盖: {bi_both}/{len(all_branch_indices)}")
    logger.info(
        f"  测试计划条目: {len(test_plan)} 条 "
        f"(baseline={sum(1 for e in test_plan.values() if e.get('source') == 'baseline')}, "
        f"mutation={sum(1 for e in test_plan.values() if e.get('source') == 'mutation')})"
    )
    logger.info(f"  变异轮次: {mutation_round}, explore_log: {len(explore_log)} 条")


def make_coverage_target_key(bi: int, value: bool) -> str:
    """返回覆盖目标的唯一键，如 '42:True'。"""
    return f"{bi}:{'True' if value else 'False'}"


def build_coverage_target_matrix(all_branch_indices: List[int], k: int) -> List[tuple]:
    """
    Step 1 — 计算覆盖目标集合。

    k=1: 对每个 branch 生成 (bi, True) 和 (bi, False) 两个目标，
         共 2*N 个单分支目标。
    k>1: 对 k 个 branch 的所有组合生成目标，每个组合内每个 branch
         都需要达到指定值，使用 True/False 的笛卡尔积。
         共 C(N,k) * 2^k 个目标。

    返回: List[tuple]，每个元素是 ((bi, target_bool), ...) 的 tuple，
          长度为 k，表示"同时满足这些 branch 取值"的一个测试目标。
    """
    if k == 1:
        targets = []
        for bi in all_branch_indices:
            targets.append(((bi, True),))
            targets.append(((bi, False),))
        return targets

    from itertools import product as _product

    targets = []
    for combo in _combinations(all_branch_indices, k):
        for values in _product([True, False], repeat=k):
            targets.append(tuple(zip(combo, values)))
    return targets