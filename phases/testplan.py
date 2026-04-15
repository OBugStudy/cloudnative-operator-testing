

import copy
import json
import logging
import os
import random as _random
from typing import Dict, List, Optional

import yaml

import cluster.apply as _cluster_apply
from acto.kubectl_client import KubectlClient
from checkpoint.store import _save_checkpoint
from cluster.apply import apply_cr_and_collect
from core.patch import _apply_patch_to_cr, _delete_field_from_cr, _parse_llm_patch
from core.rich_logger import update_progress, update_status
from core.timing import _timed_step
from crd.schema import _extract_crd_required_fields
from crd.validation import _repair_required_fields, _validate_patch_against_crd
from instrumentation.diff import (
    _extract_branch_values_from_instr,
    diff_branch_sequences,
)
from instrumentation.source import _get_branch_source_context
from llm.client import _call_llm_for_branch_flip
from llm.prompts import (
    _build_branch_flip_prompt,
    _build_diverse_cr_prompt,
    _related_fields_for_branch,
)
from relations.tracker import _update_field_relations_from_diff

logger = logging.getLogger(__name__)

STUCK_THRESHOLD = 10
SELECT_RATE = 0.1
COVERAGE_STUCK_THRESHOLD = 10
UNCOVERED_EXPLORE_RATE = 0.15


def _make_target_key(bi: int, value: bool) -> str:
    """生成单分支目标键，如 '1_T' 或 '42_F'。"""
    return f"{bi}_{'T' if value else 'F'}"


def _build_all_target_keys(all_branch_indices: List[int], k: int = 1) -> List[str]:
    """构建全量目标键列表。k=1 时每个 branch 生成 T 和 F 两个键。"""
    if k == 1:
        return [
            _make_target_key(bi, v) for bi in all_branch_indices for v in (True, False)
        ]
    from itertools import combinations as _comb
    from itertools import product as _prod

    keys = []
    for combo in _comb(all_branch_indices, k):
        for values in _prod((True, False), repeat=k):
            keys.append(
                "_".join(f"{bi}_{'T' if v else 'F'}" for bi, v in zip(combo, values))
            )
    return keys


def _build_initial_targets(target_keys: List[str]) -> dict:
    """初始化 targets 字典：每个键对应 {resolved: False, testcase_id: []}。"""
    return {k: {"resolved": False, "testcase_id": []} for k in target_keys}


def _branches_of_target_key(key: str) -> List[int]:
    """从目标键提取 branch indices，如 '1_T' → [1], '1_T_2_F' → [1, 2]。"""
    parts = key.split("_")
    return [int(parts[i]) for i in range(0, len(parts), 2)]


def _restore_testplan_state(
    tp: dict,
    all_branch_indices: List[int],
    k: int,
) -> tuple:
    """从 checkpoint 恢复 testplan 运行状态。

    返回 (coverage_map, testcases, targets, stuck_count, next_id,
           branch_history, target_history, round_n)。
    """
    coverage_map: Dict[int, bool] = {bi: False for bi in all_branch_indices}
    for bi_str, val in tp.get("coverage_map", {}).items():
        try:
            coverage_map[int(bi_str)] = bool(val)
        except (ValueError, TypeError):
            pass

    testcases: dict = tp.get("testcases", {})
    if not isinstance(testcases, dict):
        testcases = {}

    target_keys = _build_all_target_keys(all_branch_indices, k)
    targets: dict = {**_build_initial_targets(target_keys), **tp.get("targets", {})}

    stuck_count: int = tp.get("stuck_count", 0)
    next_id: int = tp.get("next_id", 1)
    branch_history: list = tp.get("branch_coverage_history", [])
    target_history: list = tp.get("target_coverage_history", [])

    round_n: int = tp.get("round_n", len(branch_history))
    return (
        coverage_map,
        testcases,
        targets,
        stuck_count,
        next_id,
        branch_history,
        target_history,
        round_n,
    )


def _save_testplan_state(
    tp: dict,
    coverage_map: Dict[int, bool],
    testcases: dict,
    targets: dict,
    stuck_count: int,
    next_id: int,
    branch_history: list,
    target_history: list,
    ckpt_path: str,
    ckpt: dict,
    round_n: int = 0,
) -> None:
    """将当前运行状态序列化回 checkpoint 并保存。"""
    tp["coverage_map"] = {str(bi): v for bi, v in coverage_map.items()}
    tp["testcases"] = testcases
    tp["targets"] = targets
    tp["stuck_count"] = stuck_count
    tp["next_id"] = next_id
    tp["branch_coverage_history"] = branch_history
    tp["target_coverage_history"] = target_history
    tp["round_n"] = round_n
    _save_checkpoint(ckpt_path, ckpt)


def _branches_seen_in_instr(instr: dict) -> set:
    """返回 instr traces 中出现的所有 branch indices 集合。"""
    return {
        t["branch_index"]
        for t in instr.get("traces", [])
        if isinstance(t, dict) and "branch_index" in t
    }


def _update_coverage_map(coverage_map: Dict[int, bool], instr: dict) -> set:
    """将 instr 中出现的分支标记为 True，返回本次新增覆盖的 branch indices。"""
    seen = _branches_seen_in_instr(instr)
    newly = {bi for bi in seen if not coverage_map.get(bi, False)}
    for bi in seen:
        if bi in coverage_map:
            coverage_map[bi] = True
    return newly


def _update_targets(
    targets: dict,
    instr_data: Optional[dict],
    testcase_id: str,
) -> List[str]:
    """根据本次 branch_values 更新 targets 的 resolved 状态。

    对每个尚未 resolved 的 target，检查其 (bi, value) 条件是否满足；
    若满足则标记 resolved=True 并记录 testcase_id。
    返回本次新 resolved 的目标键列表。
    """
    from instrumentation.diff import _validate_branch_values_from_instr

    newly_resolved = []
    for key, tgt in targets.items():
        if tgt["resolved"]:
            continue
        parts = key.split("_")
        satisfied = True
        for i in range(0, len(parts) - 1, 2):
            try:
                bi = int(parts[i])
                want = parts[i + 1] == "T"
            except (IndexError, ValueError):
                satisfied = False
                break
            satisfied = _validate_branch_values_from_instr(instr_data, bi, want)
            if not satisfied:
                break
        if satisfied:
            tgt["resolved"] = True
            tgt["testcase_id"].append(testcase_id)
            newly_resolved.append(key)
    return newly_resolved


def _record_branch_history(
    branch_history: list,
    round_n: int,
    testcase_id: str,
    newly_covered: set,
    coverage_map: Dict[int, bool],
    field_path: list | None = None,
) -> None:
    """追加一条分支覆盖历史记录。"""
    total_covered = sum(1 for v in coverage_map.values() if v)
    entry: dict = {
        "round": round_n,
        "testcase_id": testcase_id,
        "newly_covered_branches": sorted(newly_covered),
        "total_covered": total_covered,
    }
    if field_path is not None:
        entry["field_path"] = field_path
    branch_history.append(entry)


def _record_target_history(
    target_history: list,
    round_n: int,
    testcase_id: str,
    newly_resolved: List[str],
    targets: dict,
) -> None:
    """追加一条测试目标覆盖历史记录。"""
    total_resolved = sum(1 for t in targets.values() if t["resolved"])
    target_history.append(
        {
            "round": round_n,
            "testcase_id": testcase_id,
            "newly_resolved_targets": newly_resolved,
            "total_resolved": total_resolved,
        }
    )


def _select_probe_pending_target(
    testcases: dict,
    targets: dict,
    rng: _random.Random,
) -> tuple:
    """Return (testcase, target_key) for the first unresolved probe_pending target.

    Picks the probe_pending target at random, then finds the testcase with the
    lowest frequency whose involved_branches include that target's branch.
    Returns (None, None) if no such target/testcase exists.
    """
    pending_keys = [
        k
        for k, t in targets.items()
        if t.get("probe_pending") and not t.get("resolved")
    ]
    if not pending_keys:
        return None, None

    chosen_key = rng.choice(pending_keys)
    bi = _branches_of_target_key(chosen_key)[0]

    candidates = [
        tc for tc in testcases.values() if bi in set(tc.get("involved_branches", []))
    ]
    if not candidates:
        return None, chosen_key

    candidates.sort(key=lambda tc: tc["frequency"])
    return candidates[0], chosen_key


_RELATION_BOOST_RATIO = 9.0


def _build_branch_var_coverage(
    field_relations: dict, branch_meta_index: dict
) -> Dict[int, float]:
    """For each branch compute the ratio of its named variables that have known
    CR-field mappings in field_relations.  Returns {branch_index: 0.0–1.0}.

    Rationale: a branch where 1/1 variable is mapped (100%) should rank
    higher than one where 4/5 are mapped (80%), because LLM guidance is
    maximally accurate when *every* variable has a known CR path.

    For no-variable branches, fall back to expression_fmts coverage.
    """

    mapped_vars: Dict[int, set] = {}
    mapped_exprs: Dict[int, set] = {}
    for fdata in field_relations.values():
        for bi_str, vm in (fdata.get("variable_mappings") or {}).items():
            try:
                bi = int(bi_str)
            except (ValueError, TypeError):
                continue
            for vinfo in vm.values():
                vfmt = vinfo.get("variable_fmt", "") if isinstance(vinfo, dict) else ""
                if vfmt:
                    mapped_vars.setdefault(bi, set()).add(vfmt)
        for bi_str, efmts in (fdata.get("expression_fmts") or {}).items():
            try:
                bi = int(bi_str)
            except (ValueError, TypeError):
                continue
            for ef in efmts or []:
                mapped_exprs.setdefault(bi, set()).add(ef)

    _TRIVIAL = {"nil", "true", "false", "0", "1"}
    result: Dict[int, float] = {}
    for bi, bm in branch_meta_index.items():
        all_var_fmts: List[str] = []
        for expr in bm.get("Expressions", []) or []:
            for var in expr.get("variables", []) or []:
                vfmt = var.get("fmt") or var.get("raw", "")
                if vfmt and vfmt not in _TRIVIAL:
                    all_var_fmts.append(vfmt)
        if all_var_fmts:
            bi_mapped = mapped_vars.get(bi, set())
            result[bi] = sum(1 for vf in all_var_fmts if vf in bi_mapped) / len(
                all_var_fmts
            )
        else:

            expr_fmts_in_bm: List[str] = []
            for expr in bm.get("Expressions", []) or []:
                ef = expr.get("fmt") or expr.get("raw", "")
                if ef:
                    expr_fmts_in_bm.append(ef)
            if expr_fmts_in_bm:
                bi_mapped_e = mapped_exprs.get(bi, set())
                result[bi] = sum(
                    1 for ef in expr_fmts_in_bm if ef in bi_mapped_e
                ) / len(expr_fmts_in_bm)
            else:
                result[bi] = 1.0 if (bi in mapped_vars or bi in mapped_exprs) else 0.0
    return result


_ONE_SIDED_BOOST = 3.0


def _branch_priority_score(
    bi: int,
    branch_meta_index: dict,
    branch_var_coverage: Optional[Dict[int, float]] = None,
    one_sided_branches: Optional[set] = None,
) -> float:
    """Return a sampling weight for a branch index.

    Rules (applied multiplicatively):
      - Condition is exactly ``err != nil`` or ``err == nil`` → weight 0.05
        (strongly deprioritised — error checks are rarely CR-driven).
      - Condition text contains ``Spec`` → weight 5.0 (boosted — likely a
        spec field guard).
      - Variable coverage ratio r → weight × (1 + r × _RELATION_BOOST_RATIO).
        A branch with 1/1 mapped (r=1.0) scores higher than 4/5 (r=0.8).
      - [Improvement B] Branch is one-sided (covered but T or F missing)
        → weight × _ONE_SIDED_BOOST (3.0): already reachable, just flip the value.
      - Otherwise → weight 1.0 (neutral).
    """
    if not branch_meta_index:
        return 1.0
    bm = branch_meta_index.get(bi, {})
    fmt: str = (bm.get("Fmt") or bm.get("Raw") or "").strip()
    if not fmt:
        return 1.0
    fmt_norm = fmt.strip()
    if fmt_norm.lower() in ("err != nil", "err == nil"):
        return 0.05
    weight = 1.0
    if "Spec" in fmt_norm:
        weight *= 5.0
    var_ratio = (branch_var_coverage or {}).get(bi, 0.0)
    if var_ratio > 0:
        weight *= 1.0 + var_ratio * _RELATION_BOOST_RATIO

    if one_sided_branches and bi in one_sided_branches:
        weight *= _ONE_SIDED_BOOST
    return weight


def _weighted_choice(items: list, weights: list, rng: _random.Random):
    """Pick one item from *items* using the given *weights* (non-negative floats)."""
    total = sum(weights)
    if total <= 0:
        return rng.choice(items)
    r = rng.uniform(0, total)
    cumulative = 0.0
    for item, w in zip(items, weights):
        cumulative += w
        if r <= cumulative:
            return item
    return items[-1]


def _compute_one_sided_branches(testcases: dict) -> set:
    """[Improvement B] Return set of branch indices that appear in testcase pool
    but only with a single boolean value (T-only or F-only across all testcases).
    These are promising targets: already reachable, just need the opposite value.
    """
    t_seen: Dict[int, bool] = {}
    f_seen: Dict[int, bool] = {}
    for tc in testcases.values():
        for bi_str, val in (tc.get("branches") or {}).items():
            try:
                bi = int(bi_str)
            except (ValueError, TypeError):
                continue
            if bool(val):
                t_seen[bi] = True
            else:
                f_seen[bi] = True
    one_sided: set = set()
    all_seen = set(t_seen) | set(f_seen)
    for bi in all_seen:
        if bi in t_seen and bi not in f_seen:
            one_sided.add(bi)
        elif bi in f_seen and bi not in t_seen:
            one_sided.add(bi)
    return one_sided


def _select_next_test_case(
    testcases: dict,
    targets: dict,
    rng: _random.Random,
    branch_meta_index: Optional[dict] = None,
    field_relations: Optional[dict] = None,
) -> tuple:
    """Select the next test case and a target to flip.

    Selection algorithm:
      1. Build a map: branch_index -> list of unresolved target keys.
      2. Collect candidates with overlap between involved_branches and
         unresolved-target branches, split into two tiers:
           Tier-1: has_new_branch=True
           Tier-2: has_new_branch=False (admitted only for resolving targets)
      3. Within each tier, pick the testcase with the lowest frequency
         (ties broken by largest overlap count).
      4. From the chosen testcase's involved branches, collect all unresolved
         target keys, then pick one via weighted random using variable
         coverage ratio (mapped_vars/total_vars per branch).
      5. If Tier-1 yields a candidate, use it; otherwise fall back to Tier-2.
      6. Return (testcase_dict, target_key) or (None, None) if no candidate.
    """
    branch_var_coverage = (
        _build_branch_var_coverage(field_relations, branch_meta_index)
        if field_relations and branch_meta_index
        else {}
    )

    one_sided = _compute_one_sided_branches(testcases)

    unresolved_by_branch: Dict[int, List[str]] = {}
    for key, tgt in targets.items():
        if not tgt["resolved"]:
            for bi in _branches_of_target_key(key):
                unresolved_by_branch.setdefault(bi, []).append(key)

    if not unresolved_by_branch:
        return None, None

    all_unresolved_branches = set(unresolved_by_branch.keys())

    tier1: list = []
    tier2: list = []

    for tc in testcases.values():
        involved = set(tc.get("involved_branches", []))
        overlap = involved & all_unresolved_branches
        if not overlap:
            continue
        sort_key = (tc["frequency"], -len(overlap))
        if tc.get("has_new_branch", True):
            tier1.append((sort_key, tc))
        else:
            tier2.append((sort_key, tc))

    chosen_tc = None
    for tier in (tier1, tier2):
        if tier:
            tier.sort(key=lambda x: x[0])
            chosen_tc = tier[0][1]
            break

    if chosen_tc is None:
        return None, None

    involved = set(chosen_tc.get("involved_branches", []))
    candidate_keys: List[str] = []
    candidate_weights: List[float] = []
    for bi in involved:
        keys_for_bi = unresolved_by_branch.get(bi, [])
        if keys_for_bi:
            w = _branch_priority_score(
                bi, branch_meta_index, branch_var_coverage, one_sided
            )
            for k in keys_for_bi:
                candidate_keys.append(k)
                candidate_weights.append(w)

    if not candidate_keys:
        return chosen_tc, None
    chosen_key = _weighted_choice(candidate_keys, candidate_weights, rng)
    return chosen_tc, chosen_key


def _select_uncovered_branch_target(
    targets: dict,
    testcases: dict,
    rng: _random.Random,
    branch_meta_index: Optional[dict] = None,
    field_relations: Optional[dict] = None,
) -> Optional[str]:
    """Select an unresolved target key whose branch has never been covered by
    any test case — these branches are invisible to the normal selection loop.

    Branches with higher variable-coverage ratio in field_relations get
    proportionally higher sampling weight.
    Returns a target_key string or None.
    """
    covered_by_testcases: set = set()
    for tc in testcases.values():
        covered_by_testcases.update(tc.get("involved_branches", []))

    branch_var_coverage = (
        _build_branch_var_coverage(field_relations, branch_meta_index)
        if field_relations and branch_meta_index
        else {}
    )

    one_sided = _compute_one_sided_branches(testcases)

    candidate_keys: List[str] = []
    candidate_weights: List[float] = []
    for key, tgt in targets.items():
        if tgt["resolved"]:
            continue
        bis = _branches_of_target_key(key)

        if any(bi in covered_by_testcases for bi in bis):
            continue
        w = max(
            _branch_priority_score(
                bi, branch_meta_index, branch_var_coverage, one_sided
            )
            for bi in bis
        )
        candidate_keys.append(key)
        candidate_weights.append(w)

    if not candidate_keys:
        return None
    return _weighted_choice(candidate_keys, candidate_weights, rng)


def _make_testcase(
    tc_id: str,
    cr_yaml: str,
    instr: dict,
    has_new_branch: bool = True,
) -> dict:
    """构建一个新测试用例条目。

    has_new_branch=True  表示该用例带来了新分支覆盖（优先选取）。
    has_new_branch=False 表示仅因解决了新目标而入池（次优先选取）。
    """
    branch_values = _extract_branch_values_from_instr(instr)
    return {
        "id": tc_id,
        "cr": cr_yaml,
        "involved_branches": sorted(_branches_seen_in_instr(instr)),
        "frequency": 0,
        "has_new_branch": has_new_branch,
        "branches": branch_values,
    }


def _maybe_add_to_test_cases(
    testcases: dict,
    next_id: int,
    cr_yaml: str,
    instr: dict,
    coverage_map_snapshot: Dict[int, bool],
    newly_resolved: set,
    rng: _random.Random,
) -> tuple:
    """根据新分支覆盖或新解决目标情况决定是否将当前 CR 加入测试用例池。

    coverage_map_snapshot 是调用 _update_coverage_map 之前的快照。
    newly_resolved 是本轮 _update_targets 返回的新解决目标集合。
    返回 (added: bool, new_next_id: int, tc_id_if_added: str|None)。

    has_new_branch 标记规则：
      - 有新分支覆盖 → has_new_branch=True
      - 无新分支但有新解决目标 → has_new_branch=False
      - 仅随机概率准入 → has_new_branch=True（当作普通探索用例）
    """
    seen_now = _branches_seen_in_instr(instr)
    newly_covered = {bi for bi in seen_now if not coverage_map_snapshot.get(bi, False)}
    if newly_covered or rng.random() < SELECT_RATE:
        tc_id = str(next_id)
        testcases[tc_id] = _make_testcase(tc_id, cr_yaml, instr, has_new_branch=True)
        return True, next_id + 1, tc_id
    if newly_resolved:
        tc_id = str(next_id)
        testcases[tc_id] = _make_testcase(tc_id, cr_yaml, instr, has_new_branch=False)
        return True, next_id + 1, tc_id
    return False, next_id, None


def _flatten_spec_leaves(spec: dict, prefix: str = "spec") -> List[tuple]:
    """将 spec dict 展平为 [(dotpath, value), ...] 叶节点列表。"""
    leaves = []
    if not isinstance(spec, dict):
        return leaves
    for k, v in spec.items():
        path = f"{prefix}.{k}"
        if isinstance(v, dict):
            leaves.extend(_flatten_spec_leaves(v, path))
        else:
            leaves.append((path, v))
    return leaves


def _mutate_value(v, rng: _random.Random):
    """对一个叶值做简单变异，保持类型近似不变。"""
    if isinstance(v, bool):
        return not v
    if isinstance(v, int):
        return max(0, v + rng.choice([-2, -1, 1, 2, 3]))
    if isinstance(v, float):
        return v + rng.uniform(-1.0, 1.0)
    if isinstance(v, str):
        return (
            str(int(v) + 1)
            if v.isdigit()
            else (v + "-v2" if not v.endswith("-v2") else v[:-3])
        )
    if isinstance(v, list) and len(v) > 1:
        new_list = list(v)
        new_list.pop(rng.randrange(len(new_list)))
        return new_list
    return v


def _llm_generate_diverse_cr(
    seed_cr: dict,
    crd_file: str,
    cr_kind: str,
    namespace: str,
    coverage_map: Dict[int, bool],
    constraints_data: Optional[dict] = None,
) -> Optional[dict]:
    """Ask LLM to generate a structurally diverse but valid CR to escape a coverage plateau.

    Returns the new CR dict on success, or None if the LLM call or parse failed.
    """
    from llm.constraints import format_constraints_section

    uncovered_count = sum(1 for v in coverage_map.values() if not v)
    base_cr_yaml = yaml.dump(seed_cr)
    _constraints_txt = format_constraints_section(
        (constraints_data or {}).get("constraints", [])
    )
    prompt = _build_diverse_cr_prompt(
        base_cr_yaml=base_cr_yaml,
        crd_file=crd_file,
        cr_kind=cr_kind,
        uncovered_count=uncovered_count,
        constraints_txt=_constraints_txt,
    )
    with _timed_step("LLM diverse-CR", ""):
        action, llm_result = _call_llm_for_branch_flip(prompt)
    if action == "error":
        logger.warning(f"  [diverse-CR] LLM 错误: {llm_result[:200]}")
        return None
    try:
        new_cr = yaml.safe_load(llm_result)
        if not isinstance(new_cr, dict) or "spec" not in new_cr:
            logger.warning("  [diverse-CR] LLM 输出无效（缺少 spec），跳过")
            return None
        new_cr.setdefault("metadata", {})["name"] = seed_cr.get("metadata", {}).get(
            "name", "test"
        )
        new_cr.setdefault("metadata", {}).setdefault("namespace", namespace)
        return new_cr
    except Exception as e:
        logger.warning(f"  [diverse-CR] YAML 解析失败: {e}")
        return None


def _llm_try_flip(
    base_cr_yaml: str,
    bi: int,
    want_value: bool,
    branch_meta_index: dict,
    field_relations: dict,
    crd_file: str,
    cr_kind: str,
    namespace: str,
    seed_cr: dict,
    max_retries: int = 3,
    include_source_code: bool = False,
    project_path: str = "",
    instrument_dir: str = "",
    initial_error_feedback: str = "",
    combo_targets: Optional[List[dict]] = None,
    constraints_data: Optional[dict] = None,
) -> Optional[dict]:
    """用 LLM 尝试生成能让 branch[bi] 取到 want_value 的 CR 变异。

    成功时返回变异后的 CR dict；所有尝试失败时返回 None。
    initial_error_feedback: 上一轮 apply 失败的错误信息，作为第一次 LLM 调用的初始上下文。
    """
    bm = branch_meta_index.get(bi, {})
    related = _related_fields_for_branch(bi, field_relations)

    src_ctx = ""
    if project_path and instrument_dir:
        src_ctx = _get_branch_source_context(project_path, instrument_dir, bi)

    from llm.constraints import filter_constraints, format_constraints_section

    _related_fps = [r["field_path"] for r in related]
    _relevant_constraints = filter_constraints(constraints_data or {}, _related_fps)
    _constraints_txt = format_constraints_section(_relevant_constraints)
    required_fields = _extract_crd_required_fields(crd_file, cr_kind)
    error_feedback = initial_error_feedback

    _tried_set_keys: List[str] = []
    for attempt_n in range(1, max_retries + 1):
        logger.info(
            f"  [LLM flip {attempt_n}/{max_retries}] b[{bi}]→{'T' if want_value else 'F'}"
        )

        _diversity_hint = ""
        if _tried_set_keys:
            _diversity_hint = (
                f"\n[已尝试过但未改变分支取值的字段组合]: {'; '.join(_tried_set_keys)}\n"
                "请尝试完全不同的字段组合或字段值范围。"
            )
        prompt = _build_branch_flip_prompt(
            branch_meta=bm,
            current_value=None,
            target_value=want_value,
            source_context=src_ctx,
            related_fields=related,
            base_cr_yaml=base_cr_yaml,
            crd_file=crd_file,
            cr_kind=cr_kind,
            error_feedback=error_feedback + _diversity_hint,
            include_source_code=include_source_code,
            combo_targets=combo_targets or [],
            constraints_txt=_constraints_txt,
        )
        with _timed_step("LLM branch-flip", f"b[{bi}]"):
            action, llm_result = _call_llm_for_branch_flip(prompt)

        if action == "error":
            logger.warning(f"  LLM 错误: {llm_result[:200]}")
            error_feedback += "\n" + f"LLM error: {llm_result}"
            continue

        patch, parse_err = _parse_llm_patch(llm_result)
        if parse_err:
            logger.warning(f"  Patch 解析失败: {parse_err}")
            error_feedback += "\n" + f"Patch parse error: {parse_err}"
            continue
        logger.debug(f"  patch: {json.dumps(patch)}")
        patch, crd_err = _validate_patch_against_crd(patch, crd_file, cr_kind)
        if crd_err and not patch.get("set"):
            error_feedback += "\n" + crd_err
            continue


        _set_fields = sorted((patch.get("set") or {}).keys())
        _del_fields = sorted(patch.get("delete") or [])
        if _set_fields or _del_fields:
            _tried_set_keys.append(f"set={_set_fields} del={_del_fields}")

        try:
            base_cr = yaml.safe_load(base_cr_yaml) or seed_cr
            mutated = _apply_patch_to_cr(base_cr, patch)
            mutated.setdefault("metadata", {})["name"] = seed_cr["metadata"]["name"]
            mutated.setdefault("metadata", {}).setdefault("namespace", namespace)
            if required_fields:
                mutated, _repaired = _repair_required_fields(
                    mutated, base_cr, required_fields, f"spec.branch{bi}"
                )
                if _repaired:
                    logger.info(f"  已自动补回必填字段: {_repaired}")
            return mutated
        except Exception as e:
            logger.warning(f"  Patch 应用失败: {e}")
            error_feedback += "\n" + f"Patch apply error: {e}"
            continue

    logger.info(f"  LLM flip 失败（{max_retries} 次），将使用原始 CR")
    return None


def _find_warm_start_cr_from_db(
    db_dir: str,
    bi: int,
    want_value: bool,
    rng: _random.Random,
) -> Optional[str]:
    """[Improvement C] Find a CR YAML from the DB whose branch_values[bi] == want_value.

    This provides a warm-start base for LLM flip: instead of mutating from a
    testcase that never achieved the target value, we start from one that already
    has it (or the closest one), giving LLM a much better starting point.

    Returns CR YAML string, or None if no matching testcase found.
    """
    if not db_dir:
        return None
    try:
        from testcase_db.store import query_covering_branch

        candidates = query_covering_branch(db_dir, bi)
        matching = [
            tc
            for tc in candidates
            if bool(tc.get("branch_values", {}).get(str(bi))) == want_value
        ]
        if matching:
            chosen = rng.choice(matching)
            cr_yaml = chosen.get("cr", "")
            if cr_yaml:
                logger.info(
                    f"  [Improvement C] warm-start: DB 用例 {chosen.get('id')} "
                    f"已有 b[{bi}]={'T' if want_value else 'F'}"
                )
                return cr_yaml

        if candidates:
            chosen = rng.choice(candidates)
            cr_yaml = chosen.get("cr", "")
            if cr_yaml:
                logger.info(
                    f"  [Improvement C] warm-start (partial): DB 用例 {chosen.get('id')} "
                    f"覆盖 b[{bi}] 但值不符"
                )
                return cr_yaml
    except Exception as _e:
        logger.debug(f"  [Improvement C] warm-start 查询失败: {_e}")
    return None


def _random_mutate_cr(
    cr: dict, crd_file: str, cr_kind: str, rng: _random.Random
) -> dict:
    """对 CR 做一次随机字段变异（不依赖 LLM）。

    策略：从 spec 叶节点中随机选一个可选字段，以 50% 概率删除、50% 概率变异其值。
    若无可选字段则返回原 CR 的深拷贝。
    """
    required = set(_extract_crd_required_fields(crd_file, cr_kind))
    leaves = _flatten_spec_leaves(cr.get("spec", {}))
    mutable = [(path, v) for path, v in leaves if path not in required]
    if not mutable:
        return copy.deepcopy(cr)
    path, val = rng.choice(mutable)
    if rng.random() < 0.5:
        return _delete_field_from_cr(cr, path)
    new_val = _mutate_value(val, rng)
    return _apply_patch_to_cr(cr, {"set": {path: new_val}, "delete": []})


def find_branch_text_by_branch_idx(instrumet_dir, chosen_target_key):
    instr_path = os.path.join(instrumet_dir, "instrument_info.json")
    if not os.path.exists(instr_path):
        return ""
    with open(instr_path, "r") as fp:
        instr = json.load(fp)

    try:
        branch_key = int(chosen_target_key.split("_")[0])
    except Exception:
        return ""

    for bp in instr.get("branch_points", []):
        if bp["BranchIndex"] == branch_key:
            return bp["Fmt"]
    print(f"Fail to find text for {branch_key}")
    return ""


def run_testplan_phase(
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
    field_relations: Optional[dict] = None,
    k: int = 1,
    max_rounds: int = 0,
    max_retries: int = 3,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    config_path: str = "",
    include_source_code: bool = False,
    project_path: str = "",
    instrument_dir: str = "",
    rebuild_cluster_fn=None,
    instrument_prefix: str = "",
    declared_field_paths: Optional[set] = None,
    constraints_data: Optional[dict] = None,
    db_dir: str = "",
) -> None:
    """TestPlan 主流程：测试用例池驱动的分支覆盖探索。"""
    logger.info("=" * 70)
    logger.info("TestPlan — 测试用例池驱动分支覆盖探索")
    logger.info("=" * 70)
    update_status(phase="TestPlan", current_op="初始化")

    tp = ckpt.setdefault(
        "testplan",
        {
            "coverage_map": {},
            "testcases": {},
            "targets": {},
            "stuck_count": 0,
            "next_id": 1,
            "branch_coverage_history": [],
            "target_coverage_history": [],
            "baseline_collected": False,
        },
    )
    llm_stats = tp.setdefault(
        "llm_stats",
        {"cr_gen_attempts": 0, "cr_gen_produced": 0, "cr_apply_success": 0},
    )
    target_hit_stats = tp.setdefault("target_hit_stats", {"attempts": 0, "hits": 0})

    all_branch_indices = sorted(branch_meta_index.keys())
    target_keys = _build_all_target_keys(all_branch_indices, k)
    total_targets = len(target_keys)
    _n = len(all_branch_indices)
    _formula = f"{_n} branch × 2" if k == 1 else f"C({_n},{k}) × 2^{k}"
    logger.info(f"[Step 1] k={k}, 总目标数: {total_targets}  ({_formula})")

    (
        coverage_map,
        testcases,
        targets,
        stuck_count,
        next_id,
        branch_history,
        target_history,
        round_n,
    ) = _restore_testplan_state(tp, all_branch_indices, k)

    seed_cr_yaml = yaml.dump(seed_cr)
    rng = _random.Random()
    _baseline_instr: Optional[dict] = None


    _db_global_coverage: set = set()
    _db_recorded_count = 0
    _db_recorded_ids: List[str] = []
    _db_skipped_count = 0
    if db_dir:
        try:
            from testcase_db.store import _load_index as _db_load_index

            _dbi = _db_load_index(db_dir)
            _db_global_coverage = {int(k) for k in _dbi.keys()}
            logger.info(
                f"[testcase_db] 已加载全局覆盖集合: {len(_db_global_coverage)} 个已覆盖分支"
            )
        except Exception as _dbe:
            logger.warning(f"[testcase_db] 加载全局覆盖集合失败: {_dbe}")

    if not tp.get("baseline_collected"):
        logger.info("[Step 2] 应用 seed CR，收集基准插桩数据...")
        update_status(current_op="收集基准数据")
        instr, _, ok, _, _dead = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=seed_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
        )
        if _dead:
            logger.error("[Step 2] 控制器 Pod 无法恢复，尝试重建集群...")
            if rebuild_cluster_fn is not None:
                rebuild_cluster_fn()
                logger.warning("[Step 2] 集群已重建，请重新运行 TestPlan")
            return
        if not ok or instr is None:
            logger.error("[Step 2] 基准数据收集失败，终止 TestPlan")
            return
        _baseline_instr = instr
        tc_id = str(next_id)
        next_id += 1
        testcases[tc_id] = _make_testcase(tc_id, seed_cr_yaml, instr)

        if db_dir:
            try:
                from testcase_db.store import record_testcase as _db_record

                _sid = _db_record(
                    db_dir=db_dir,
                    cr_yaml=seed_cr_yaml,
                    instr_data=instr,
                    source="testplan",
                    global_coverage_set=_db_global_coverage,
                )
                if _sid:
                    _db_recorded_count += 1
                    _db_recorded_ids.append(tc_id)
                    from testcase_db.store import _extract_covered_branches as _ecb

                    for _bi in _ecb(instr):
                        _db_global_coverage.add(_bi)
            except Exception as _dbe:
                logger.debug(f"[testcase_db] seed CR 记录失败: {_dbe}")
        newly_covered = _update_coverage_map(coverage_map, instr)
        newly_resolved = _update_targets(targets, instr, tc_id)
        _record_branch_history(branch_history, 0, tc_id, newly_covered, coverage_map)
        _record_target_history(target_history, 0, tc_id, newly_resolved, targets)
        tp["baseline_collected"] = True
        _save_testplan_state(
            tp,
            coverage_map,
            testcases,
            targets,
            stuck_count,
            next_id,
            branch_history,
            target_history,
            ckpt_path,
            ckpt,
            round_n=0,
        )
        total_covered = sum(1 for v in coverage_map.values() if v)
        total_resolved = sum(1 for t in targets.values() if t["resolved"])
        logger.info(
            f"[Step 2] 基准完成: 覆盖分支 {total_covered}/{len(all_branch_indices)}, "
            f"已解决目标 {total_resolved}/{total_targets}"
        )
    else:
        logger.info("[Step 2] 使用 checkpoint 中的基准数据")
        _baseline_instr = tp.get("_baseline_instr")


    if db_dir and not tp.get("db_bootstrap_done") and len(testcases) <= 1:
        logger.info(
            "[Step 2.5][Improvement A] 从 testcase_db 批量导入历史用例初始化用例池..."
        )
        _bootstrap_count = 0
        _bootstrap_resolved = 0
        try:
            from testcase_db.store import iter_all as _db_iter_all

            for _db_tc in _db_iter_all(db_dir):
                _db_cr = _db_tc.get("cr", "")
                _db_bvs = _db_tc.get("branch_values", {})
                _db_cbs = _db_tc.get("covered_branches", [])
                if not _db_cr or not _db_cbs:
                    continue

                _fake_instr = {
                    "traces": [
                        {"branch_index": int(bi), "value": val}
                        for bi, val in _db_bvs.items()
                        if (isinstance(bi, str) and bi.isdigit()) or isinstance(bi, int)
                    ]
                }

                for _bi in _db_cbs:
                    if isinstance(_bi, int) and _bi in coverage_map:
                        coverage_map[_bi] = True
                        _db_global_coverage.add(_bi)

                _bs_id = f"db_{_bootstrap_count}"
                _bs_resolved = _update_targets(targets, _fake_instr, _bs_id)
                _bootstrap_resolved += len(_bs_resolved)

                _bs_tc_id = str(next_id)
                next_id += 1
                _bs_tc = {
                    "id": _bs_tc_id,
                    "cr": _db_cr,
                    "involved_branches": sorted(
                        [int(b) for b in _db_cbs if isinstance(b, int)]
                    ),
                    "frequency": 0,
                    "has_new_branch": True,
                    "branches": {str(k): v for k, v in _db_bvs.items()},
                }
                testcases[_bs_tc_id] = _bs_tc
                _bootstrap_count += 1
        except Exception as _dbe:
            logger.warning(f"[Step 2.5][Improvement A] DB 导入失败: {_dbe}")
        tp["db_bootstrap_done"] = True
        total_covered_bs = sum(1 for v in coverage_map.values() if v)
        total_resolved_bs = sum(1 for t in targets.values() if t["resolved"])
        logger.info(
            f"[Step 2.5][Improvement A] 导入 {_bootstrap_count} 个历史用例, "
            f"解决目标 {_bootstrap_resolved} 个 | "
            f"覆盖: {total_covered_bs}/{len(all_branch_indices)} | "
            f"已解决: {total_resolved_bs}/{total_targets}"
        )
        _save_testplan_state(
            tp,
            coverage_map,
            testcases,
            targets,
            stuck_count,
            next_id,
            branch_history,
            target_history,
            ckpt_path,
            ckpt,
            round_n=round_n,
        )


    logger.info("[Step 3] 开始主循环")
    round_n = len(branch_history)
    _skip_reset = False
    _last_apply_error: str = (
        ""
    )
    _last_flip_target: Optional[str] = None
    _coverage_stuck_count: int = 0

    while True:
        total_resolved = sum(1 for t in targets.values() if t["resolved"])
        total_covered = sum(1 for v in coverage_map.values() if v)
        update_progress(
            done=total_resolved,
            total=total_targets,
            label="目标",
            branches_covered=total_covered,
            branches_total=len(all_branch_indices),
        )

        if total_resolved >= total_targets:
            logger.info("[Step 3] 所有测试目标已解决，退出")
            break
        if max_rounds > 0 and round_n >= max_rounds:
            logger.info(f"[Step 3] 已达 max_rounds={max_rounds}，退出")
            break


        _llm_cr_generated_this_round = False
        _flip_target_key_this_round: Optional[str] = None


        _applying_diverse_cr = False
        if _coverage_stuck_count >= COVERAGE_STUCK_THRESHOLD:
            _coverage_stuck_count = 0
            logger.info(
                f"[Step 3] 分支覆盖连续 {COVERAGE_STUCK_THRESHOLD} 轮无新增，"
                "调用 LLM 生成多样化 CR..."
            )
            update_status(current_op="LLM 多样化 CR 生成")
            llm_stats["cr_gen_attempts"] += 1
            diverse_cr = _llm_generate_diverse_cr(
                seed_cr=seed_cr,
                crd_file=crd_file,
                cr_kind=cr_kind,
                namespace=namespace,
                coverage_map=coverage_map,
                constraints_data=constraints_data,
            )
            if diverse_cr is not None:
                llm_stats["cr_gen_produced"] += 1
                _llm_cr_generated_this_round = True
                current_cr = diverse_cr
                current_cr_yaml = yaml.dump(diverse_cr)
                selected_id = None
                chosen_target_key = None
                _applying_diverse_cr = True
                logger.info("  [diverse-CR] 多样化 CR 生成成功，将在本轮应用")
            else:
                logger.warning("  [diverse-CR] 生成失败，继续正常流程")

        if not _applying_diverse_cr:

            probe_selected, probe_key = _select_probe_pending_target(
                testcases, targets, rng
            )
            if probe_selected is not None or probe_key is not None:
                selected, chosen_target_key = probe_selected, probe_key
                if probe_key:
                    logger.info(f"[Step 3] probe_pending 优先目标: {probe_key}")
            else:

                _try_uncovered = (
                    field_relations and rng.random() < UNCOVERED_EXPLORE_RATE
                )
                if _try_uncovered:
                    _unc_key = _select_uncovered_branch_target(
                        targets,
                        testcases,
                        rng,
                        branch_meta_index=branch_meta_index,
                        field_relations=field_relations,
                    )
                    if _unc_key is not None:
                        selected = None
                        chosen_target_key = _unc_key
                        logger.info(f"[Step 3] 探索未覆盖分支目标: {_unc_key}")
                    else:
                        selected, chosen_target_key = _select_next_test_case(
                            testcases,
                            targets,
                            rng,
                            branch_meta_index=branch_meta_index,
                            field_relations=field_relations,
                        )
                else:
                    selected, chosen_target_key = _select_next_test_case(
                        testcases,
                        targets,
                        rng,
                        branch_meta_index=branch_meta_index,
                        field_relations=field_relations,
                    )
            if selected is None and chosen_target_key is None:

                stuck_count += 1
                logger.info(
                    f"[Step 3] 无可选测试用例，随机变异 seed CR "
                    f"(stuck={stuck_count}/{STUCK_THRESHOLD})"
                )
                if stuck_count > STUCK_THRESHOLD:
                    logger.info("[Step 3] 超过卡住阙値，退出")
                    break
                update_status(current_op=f"随机变异 (stuck={stuck_count})")
                current_cr = _random_mutate_cr(seed_cr, crd_file, cr_kind, rng)
                current_cr_yaml = yaml.dump(current_cr)
                selected_id = None
            else:


                stuck_count = 0
                if selected is not None:
                    selected_id = selected["id"]
                    selected["frequency"] += 1
                else:
                    selected_id = None

                _flip_target_key_this_round = chosen_target_key

                current_cr = None
                if chosen_target_key is not None and field_relations:
                    parts = chosen_target_key.split("_")
                    try:
                        bi_flip = int(parts[0])
                        want_flip = parts[1] == "T"

                        _combo_targets: List[dict] = []
                        for _ci in range(2, len(parts) - 1, 2):
                            _cb_bi = int(parts[_ci])
                            _cb_want = parts[_ci + 1] == "T"
                            _cb_bm = branch_meta_index.get(_cb_bi, {})
                            _cb_rel = _related_fields_for_branch(
                                _cb_bi, field_relations
                            )
                            _combo_targets.append(
                                {
                                    "branch_meta": _cb_bm,
                                    "target_value": _cb_want,
                                    "related_fields": _cb_rel,
                                }
                            )
                        _tgt_label = chosen_target_key
                        update_status(current_op=f"LLM {_tgt_label}")

                        if chosen_target_key != _last_flip_target:
                            _last_apply_error = ""
                        _carry_error = _last_apply_error
                        llm_stats["cr_gen_attempts"] += 1

                        if selected is not None:
                            _base_for_flip = selected["cr"]
                        else:
                            _warm_cr_yaml = _find_warm_start_cr_from_db(
                                db_dir, bi_flip, want_flip, rng
                            )
                            _base_for_flip = (
                                _warm_cr_yaml if _warm_cr_yaml else seed_cr_yaml
                            )
                        current_cr = _llm_try_flip(
                            base_cr_yaml=_base_for_flip,
                            bi=bi_flip,
                            want_value=want_flip,
                            branch_meta_index=branch_meta_index,
                            field_relations=field_relations,
                            crd_file=crd_file,
                            cr_kind=cr_kind,
                            namespace=namespace,
                            seed_cr=seed_cr,
                            max_retries=max_retries,
                            include_source_code=include_source_code,
                            project_path=project_path,
                            instrument_dir=instrument_dir,
                            initial_error_feedback=_carry_error,
                            combo_targets=_combo_targets or None,
                            constraints_data=constraints_data,
                        )
                        if current_cr is not None:
                            llm_stats["cr_gen_produced"] += 1
                            _llm_cr_generated_this_round = True
                        _last_flip_target = chosen_target_key
                    except (IndexError, ValueError):
                        pass

                text = find_branch_text_by_branch_idx(
                    instrumet_dir=instrument_dir, chosen_target_key=chosen_target_key
                )
                if current_cr is not None:

                    current_cr_yaml = yaml.dump(current_cr)
                    update_status(
                        current_op=f"{'testcase[' + str(selected_id) + ']+LLM' if selected else 'uncovered+LLM'} f={selected['frequency'] if selected else 0} {chosen_target_key} text: {text}"
                    )
                else:


                    if selected is not None:
                        _base_cr_yaml = selected["cr"]
                    else:
                        _warm_fb = None
                        try:
                            _fb_parts = (chosen_target_key or "").split("_")
                            if len(_fb_parts) >= 2:
                                _fb_bi = int(_fb_parts[0])
                                _fb_want = _fb_parts[1] == "T"
                                _warm_fb = _find_warm_start_cr_from_db(
                                    db_dir, _fb_bi, _fb_want, rng
                                )
                        except (IndexError, ValueError):
                            pass
                        _base_cr_yaml = _warm_fb if _warm_fb else seed_cr_yaml
                    current_cr_yaml = _base_cr_yaml
                    current_cr = yaml.safe_load(current_cr_yaml) or seed_cr
                    update_status(
                        current_op=f"{'testcase[' + str(selected_id) + ']' if selected else 'uncovered'} f={selected['frequency'] if selected else 0} {chosen_target_key} text: {text}"
                    )

        logger.info(
            f"\n[round {round_n + 1}] "
            f"{'diverse-CR' if _applying_diverse_cr else ('testcase[' + str(selected_id) + ']' if selected_id else ('uncovered-' + str(chosen_target_key) if chosen_target_key else 'random-mutate'))}"
        )

        instr, _, ok, _is_rejection, _cluster_dead = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=current_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            skip_cluster_reset=_skip_reset,
            instrument_prefix=instrument_prefix,
        )
        _skip_reset = _is_rejection
        if _cluster_dead:
            logger.error("[Step 3] 控制器 Pod 无法恢复，尝试重建集群...")
            if rebuild_cluster_fn is None:
                logger.error("[Step 3] 未配置集群重建函数，终止 TestPlan")
                break
            new_env = rebuild_cluster_fn()
            if new_env is None:
                logger.error("[Step 3] 集群重建失败，终止 TestPlan")
                break
            kubectl_client = new_env["kubectl_client"]
            namespace = new_env["namespace"]
            cluster_name = new_env["cluster_name"]
            operator_container_name = new_env["operator_container_name"]
            _skip_reset = False
            continue
        if not ok or instr is None:
            err = _cluster_apply._last_create_stderr

            _last_apply_error = (
                (_last_apply_error + "\n" + err).strip() if _last_apply_error else err
            )
            logger.warning(f"  apply/collect 失败: {(err or '').strip()[:200]}")
            _save_testplan_state(
                tp,
                coverage_map,
                testcases,
                targets,
                stuck_count,
                next_id,
                branch_history,
                target_history,
                ckpt_path,
                ckpt,
                round_n=round_n,
            )
            continue

        _last_apply_error = ""
        if _llm_cr_generated_this_round:
            llm_stats["cr_apply_success"] += 1
        if _applying_diverse_cr:
            _last_flip_target = None


        _db_tc_id_this_round: Optional[str] = None
        if db_dir:
            try:
                from testcase_db.store import record_testcase as _db_record

                _db_tc_id_this_round = _db_record(
                    db_dir=db_dir,
                    cr_yaml=current_cr_yaml,
                    instr_data=instr,
                    source="testplan",
                    global_coverage_set=_db_global_coverage,
                )
                if _db_tc_id_this_round:
                    _db_recorded_count += 1
                    from testcase_db.store import _extract_covered_branches as _ecb

                    for _bi in _ecb(instr):
                        _db_global_coverage.add(_bi)
                else:
                    _db_skipped_count += 1
            except Exception as _dbe:
                logger.debug(f"[testcase_db] 记录失败: {_dbe}")


        if _baseline_instr is not None and field_relations is not None:
            try:
                _rel_diff = diff_branch_sequences(_baseline_instr, instr)
                _update_field_relations_from_diff(
                    field_relations=field_relations,
                    diff=_rel_diff,
                    cr_before=seed_cr,
                    cr_after=current_cr,
                    mutation_round=f"tp-inline-r{round_n + 1}",
                    branch_meta_index=branch_meta_index,
                    declared_field_paths=declared_field_paths,
                )
                ckpt["field_relations"] = field_relations
            except Exception as _rel_exc:
                logger.debug(f"[inline-explore] field_relations 更新失败: {_rel_exc}")


        coverage_map_snapshot = dict(coverage_map)
        newly_covered = _update_coverage_map(coverage_map, instr)

        _probe_id = str(next_id)
        newly_resolved = _update_targets(targets, instr, _probe_id)
        if _flip_target_key_this_round is not None:
            target_hit_stats["attempts"] += 1
            if _flip_target_key_this_round in newly_resolved:
                target_hit_stats["hits"] += 1
        added, next_id, admitted_id = _maybe_add_to_test_cases(
            testcases,
            next_id,
            current_cr_yaml,
            instr,
            coverage_map_snapshot,
            newly_resolved,
            rng,
        )
        if _db_tc_id_this_round and admitted_id:
            _db_recorded_ids.append(admitted_id)
        if not added:

            _real_id = selected_id or "random"
            for tgt in targets.values():
                tgt["testcase_id"] = [
                    (_real_id if x == _probe_id else x) for x in tgt["testcase_id"]
                ]
            newly_resolved = {k for k in newly_resolved}
        new_tc_id = admitted_id if added else (selected_id or "random")


        if chosen_target_key and targets.get(chosen_target_key, {}).get(
            "probe_pending"
        ):
            targets[chosen_target_key].pop("probe_pending", None)
            if chosen_target_key in newly_resolved:
                logger.info(
                    f"  probe_pending 目标已解决，清除标记: {chosen_target_key}"
                )
            else:
                logger.info(
                    f"  probe_pending 目标本轮未解决，清除标记: {chosen_target_key}"
                )


        round_n += 1
        _record_branch_history(
            branch_history, round_n, new_tc_id, newly_covered, coverage_map
        )
        _record_target_history(
            target_history, round_n, new_tc_id, newly_resolved, targets
        )


        if len(newly_covered) > 0:
            _coverage_stuck_count = 0
            if _applying_diverse_cr:
                seed_cr = current_cr
                seed_cr_yaml = yaml.dump(seed_cr)
                logger.info(
                    f"  [diverse-CR] 覆盖了 {len(newly_covered)} 个新分支，"
                    "已将其设为新 seed_cr"
                )
        else:
            _coverage_stuck_count += 1

        total_covered_now = sum(1 for v in coverage_map.values() if v)
        total_resolved_now = sum(1 for t in targets.values() if t["resolved"])
        _db_log = ""
        if db_dir:
            if _db_tc_id_this_round:
                _db_log = f" | DB记录={_db_tc_id_this_round}"
            else:
                _db_log = " | DB跳过"
        logger.info(
            f"  新增覆盖分支: {len(newly_covered)}, 新解决目标: {len(newly_resolved)}"
            f" | 覆盖: {total_covered_now}/{len(all_branch_indices)}"
            f" | 已解决: {total_resolved_now}/{total_targets}"
            + (f" | 已加入用例池 (id={new_tc_id})" if added else "")
            + _db_log
        )

        _save_testplan_state(
            tp,
            coverage_map,
            testcases,
            targets,
            stuck_count,
            next_id,
            branch_history,
            target_history,
            ckpt_path,
            ckpt,
            round_n=round_n,
        )


    total_resolved_final = sum(1 for t in targets.values() if t["resolved"])
    total_covered_final = sum(1 for v in coverage_map.values() if v)
    logger.info("\nTestPlan 完成:")
    logger.info(
        f"  测试目标解决: {total_resolved_final}/{total_targets} "
        f"({100 * total_resolved_final // total_targets if total_targets else 0}%)"
    )
    logger.info(f"  分支覆盖: {total_covered_final}/{len(all_branch_indices)}")
    logger.info(f"  测试用例池大小: {len(testcases)}")
    logger.info(f"  总轮次: {round_n}")
    logger.info(
        f"  历史记录: 分支={len(branch_history)} 条, 目标={len(target_history)} 条"
    )
    if db_dir:
        logger.info(
            f"  [testcase_db] TestPlan 共记录 {_db_recorded_count} 个新用例，"
            f"跳过 {_db_skipped_count} 个（重复或无新分支）"
        )
        if _db_recorded_ids:
            _pool_recorded = [_id for _id in _db_recorded_ids if _id in testcases]
            _pool_not_recorded = [
                _id for _id in testcases if _id not in _db_recorded_ids
            ]
            logger.info(
                f"  [testcase_db] 用例池中已记录: {len(_pool_recorded)} 个, "
                f"未记录（无新分支或重复）: {len(_pool_not_recorded)} 个"
            )


def explain_selection(
    testcases: dict,
    targets: dict,
    branch_meta: dict,
    field_relations: dict,
) -> dict:
    """Run the testplan TC-selection algorithm and return a rich explanation dict.

    Mirrors the production selection logic but exposes all intermediate results
    for use by the testplan debugger UI.  Returns a dict with keys:
      no_unresolved, tier1, tier2, chosen_tc, chosen_tier, chosen_key,
      target_weights, unresolved_count, resolved_count.
    """
    br_count = (
        _build_branch_var_coverage(field_relations, branch_meta)
        if field_relations and branch_meta
        else {}
    )

    unresolved_by_branch: dict[int, list] = {}
    for key, tgt in targets.items():
        if not tgt.get("resolved"):
            for bi in _branches_of_target_key(key):
                unresolved_by_branch.setdefault(bi, []).append(key)

    if not unresolved_by_branch:
        return {
            "no_unresolved": True,
            "tier1": [],
            "tier2": [],
            "chosen_tc": None,
            "target_weights": [],
        }

    all_unresolved = set(unresolved_by_branch.keys())
    tier1: list = []
    tier2: list = []

    for tc in testcases.values():
        involved = set(tc.get("involved_branches", []))
        overlap = involved & all_unresolved
        if not overlap:
            continue
        sort_key = (tc["frequency"], -len(overlap))
        entry = {
            "id": tc["id"],
            "frequency": tc["frequency"],
            "overlap_count": len(overlap),
            "overlap_branches": sorted(overlap),
            "has_new_branch": tc.get("has_new_branch", True),
            "sort_key": list(sort_key),
            "all_branches": sorted(involved),
            "cr_preview": (tc.get("cr") or "")[:400],
        }
        if tc.get("has_new_branch", True):
            tier1.append((sort_key, entry))
        else:
            tier2.append((sort_key, entry))

    tier1.sort(key=lambda x: x[0])
    tier2.sort(key=lambda x: x[0])

    chosen_entry = None
    chosen_tier_name = None
    chosen_tc_raw = None
    for tier, name in ((tier1, "Tier-1"), (tier2, "Tier-2")):
        if tier:
            chosen_entry = tier[0][1]
            chosen_tier_name = name
            chosen_tc_raw = testcases.get(chosen_entry["id"])
            break

    target_weights: list = []
    if chosen_tc_raw:
        involved = set(chosen_tc_raw.get("involved_branches", []))
        for bi in involved:
            keys_for_bi = unresolved_by_branch.get(bi, [])
            if not keys_for_bi:
                continue
            w = _branch_priority_score(bi, branch_meta, br_count)
            bm_entry = branch_meta.get(bi) or branch_meta.get(str(bi)) or {}
            for k in keys_for_bi:
                target_weights.append(
                    {
                        "key": k,
                        "bi": bi,
                        "weight": round(w, 4),
                        "fmt": bm_entry.get("Fmt") or bm_entry.get("Raw") or "",
                        "file": bm_entry.get("File") or bm_entry.get("FilePath") or "",
                        "line": bm_entry.get("Line")
                        or bm_entry.get("BranchLine")
                        or "",
                    }
                )
        target_weights.sort(key=lambda x: x["weight"], reverse=True)

    import random as _rnd

    chosen_key = None
    if target_weights:
        items = [t["key"] for t in target_weights]
        weights = [t["weight"] for t in target_weights]
        total = sum(weights)
        r = _rnd.Random().uniform(0, total)
        cumulative = 0.0
        for item, w in zip(items, weights):
            cumulative += w
            if r <= cumulative:
                chosen_key = item
                break
        if chosen_key is None:
            chosen_key = items[-1]

    return {
        "no_unresolved": False,
        "tier1": [e for _, e in tier1],
        "tier2": [e for _, e in tier2],
        "chosen_tc": chosen_entry,
        "chosen_tier": chosen_tier_name,
        "chosen_key": chosen_key,
        "target_weights": target_weights,
        "unresolved_count": len([t for t in targets.values() if not t.get("resolved")]),
        "resolved_count": len([t for t in targets.values() if t.get("resolved")]),
    }


def target_summary(
    targets: dict,
    branch_meta: dict,
    testcases: dict | None = None,
) -> list:
    """Build a display-ready list of target rows for the debugger grid.

    Each row dict has keys: key, bi, want, resolved, testcase_ids,
    probe_pending, fmt, file, line, tc_count, all_bis, conditions.
    For k>1 targets (e.g. '42_T_87_F'), all branch indices and their
    condition texts are captured in all_bis / conditions.
    """
    bi_tc_count: dict[int, int] = {}
    if testcases:
        for tc in testcases.values():
            for bi in set(tc.get("involved_branches", [])):
                bi_tc_count[bi] = bi_tc_count.get(bi, 0) + 1

    rows = []
    for key, tgt in targets.items():
        parts = key.split("_")

        bi_want_pairs: list[tuple[int, str]] = []
        i = 0
        while i + 1 < len(parts):
            if parts[i].isdigit() and parts[i + 1] in ("T", "F"):
                bi_want_pairs.append((int(parts[i]), parts[i + 1]))
                i += 2
            else:
                i += 1
        if not bi_want_pairs:
            bi_want_pairs = [(0, "?")]


        bi, want = bi_want_pairs[0]
        bm0 = branch_meta.get(bi) or branch_meta.get(str(bi)) or {}


        conditions = []
        all_bis = []
        for _bi, _want in bi_want_pairs:
            all_bis.append(_bi)
            _bm = branch_meta.get(_bi) or branch_meta.get(str(_bi)) or {}
            conditions.append(
                {
                    "bi": _bi,
                    "want": _want,
                    "fmt": _bm.get("Fmt") or _bm.get("Raw") or "",
                    "file": _bm.get("File") or _bm.get("FilePath") or "",
                    "line": _bm.get("Line") or _bm.get("BranchLine") or "",
                }
            )


        if len(conditions) == 1:
            fmt = conditions[0]["fmt"]
        else:
            fmt = (
                " ∧ ".join(
                    f"b[{c['bi']}]={'T' if c['want'] == 'T' else 'F'}: {c['fmt']}"
                    for c in conditions
                    if c["fmt"]
                )
                or key
            )


        tc_count = max((bi_tc_count.get(_bi, 0) for _bi in all_bis), default=0)

        rows.append(
            {
                "key": key,
                "bi": bi,
                "want": want,
                "resolved": tgt.get("resolved", False),
                "testcase_ids": tgt.get("testcase_id", []),
                "probe_pending": tgt.get("probe_pending", False),
                "fmt": fmt,
                "file": bm0.get("File") or bm0.get("FilePath") or "",
                "line": bm0.get("Line") or bm0.get("BranchLine") or "",
                "tc_count": tc_count,
                "all_bis": all_bis,
                "conditions": conditions,
            }
        )
    rows.sort(key=lambda r: (r["bi"], r["want"]))
    return rows