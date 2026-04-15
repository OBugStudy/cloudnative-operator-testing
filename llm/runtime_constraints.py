

import json
import logging
import os
from typing import Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

_RUNTIME_CONSTRAINT_FILE = "runtime_constraints.json"


TRACE_SHORTFALL_ABS = 100
TRACE_SHORTFALL_RATIO = 0.35


def _trace_len(instr: Optional[dict]) -> int:
    """Return number of trace entries in an instrumentation snapshot."""
    if not instr:
        return 0
    return len(instr.get("traces", []))


def check_trace_shortfall(
    before_instr: Optional[dict],
    after_instr: Optional[dict],
    abs_threshold: int = TRACE_SHORTFALL_ABS,
    ratio_threshold: float = TRACE_SHORTFALL_RATIO,
) -> bool:
    """Return True if after_instr is significantly shorter than before_instr.

    Triggers when EITHER condition holds:
    - after_len < before_len - abs_threshold
    - after_len < before_len * (1 - ratio_threshold)   [and before_len > 0]
    """
    before_len = _trace_len(before_instr)
    after_len = _trace_len(after_instr)
    if before_len == 0:
        return False
    abs_drop = before_len - after_len
    ratio_drop = abs_drop / before_len
    if abs_drop >= abs_threshold or ratio_drop >= ratio_threshold:
        logger.info(
            f"[runtime] 轨迹缩短检测: before={before_len} after={after_len} "
            f"drop={abs_drop}({ratio_drop:.0%})"
        )
        return True
    return False


def find_divergence_branch(
    before_instr: Optional[dict],
    after_instr: Optional[dict],
) -> Optional[int]:
    """Find the first branch_index that disappears (removed) after mutation.

    We walk both trace lists in order and return the branch_index of the first
    entry present in before but absent in after.  This is the likely divergence
    point where the operator exited early.
    """
    if not before_instr or not after_instr:
        return None

    before_traces = before_instr.get("traces", [])
    after_set = {t["branch_index"] for t in after_instr.get("traces", [])}

    for t in before_traces:
        bi = t.get("branch_index")
        if bi is not None and bi not in after_set:
            return bi
    return None


_DIAGNOSE_PROMPT = """\
You are a Kubernetes operator expert analyzing why a CR mutation caused the operator \
to exit early (the execution trace became significantly shorter).

## Context
The operator's execution trace shrank after applying the new CR:
- Before trace length: {before_len}
- After trace length:  {after_len}

## CR Diff (before → after)
```yaml
{cr_diff}
```

## Divergence Point (first branch that disappeared)
Branch index: {branch_index}
Branch expression: {branch_fmt}
Branch source context:
```
{source_context}
```

Branch values:
- Before: {branch_before_value}
- After: (branch no longer reached)

## Task
1. Determine if the CR mutation violated a semantic constraint that caused the operator to exit early.
2. If yes, output a YAML patch to fix the CR so it is semantically valid (the operator should reach normal steady state).
3. Also output a one-sentence constraint rule describing what was violated.

## Output format (YAML, no markdown fences, no extra text):
diagnosis: "<one sentence: was this a semantic violation? yes/no and why>"
is_violation: true | false
constraint:
  type: "<required|mutual_exclusion|co_required|conditional|enum|format|range|precedence|immutable>"
  severity: "error"
  fields: ["<field_path>", ...]
  rule: "<one sentence constraint description>"
  check: "<pseudocode check expression>"
  fix_hint: "<how to fix>"
fix_patch:
  set:
    <field_path>: <value>
  delete: []
"""


def _build_diagnose_prompt(
    before_instr: dict,
    after_instr: dict,
    base_cr: dict,
    mutated_cr: dict,
    branch_index: int,
    branch_meta_index: Optional[dict],
    project_path: str,
    instrument_dir: str,
) -> str:
    from instrumentation.source import _get_branch_source_context

    before_len = _trace_len(before_instr)
    after_len = _trace_len(after_instr)


    base_yaml = yaml.dump(base_cr, allow_unicode=True)
    mut_yaml = yaml.dump(mutated_cr, allow_unicode=True)
    import difflib
    cr_diff_lines = list(
        difflib.unified_diff(
            base_yaml.splitlines(keepends=True),
            mut_yaml.splitlines(keepends=True),
            fromfile="before.yaml",
            tofile="after.yaml",
        )
    )
    cr_diff = "".join(cr_diff_lines) or "(no diff)"


    bm = (branch_meta_index or {}).get(branch_index, {})
    branch_fmt = bm.get("Fmt") or bm.get("Raw") or f"branch[{branch_index}]"


    branch_before_value = "unknown"
    for t in before_instr.get("traces", []):
        if t.get("branch_index") == branch_index:
            branch_before_value = str(t.get("value", "unknown"))
            break


    src_ctx = _get_branch_source_context(project_path, instrument_dir, branch_index)

    return _DIAGNOSE_PROMPT.format(
        before_len=before_len,
        after_len=after_len,
        cr_diff=cr_diff,
        branch_index=branch_index,
        branch_fmt=branch_fmt,
        source_context=src_ctx or "(not available)",
        branch_before_value=branch_before_value,
    )


def diagnose_and_fix_cr(
    base_cr: dict,
    mutated_cr: dict,
    before_instr: dict,
    after_instr: dict,
    branch_index: int,
    branch_meta_index: Optional[dict],
    project_path: str,
    instrument_dir: str,
) -> Tuple[bool, Optional[dict], Optional[dict]]:
    """Ask LLM to diagnose semantic violation and return a fixed CR.

    Returns (is_violation, fixed_cr_or_None, constraint_dict_or_None).
    """
    from llm.client import _call_llm_for_branch_flip

    prompt = _build_diagnose_prompt(
        before_instr=before_instr,
        after_instr=after_instr,
        base_cr=base_cr,
        mutated_cr=mutated_cr,
        branch_index=branch_index,
        branch_meta_index=branch_meta_index,
        project_path=project_path,
        instrument_dir=instrument_dir,
    )

    logger.info(f"[runtime] 调用 LLM 诊断语义违规 (divergence branch={branch_index})...")
    action, result = _call_llm_for_branch_flip(prompt)
    if action == "error":
        logger.warning(f"[runtime] LLM 诊断失败: {result[:200]}")
        return False, None, None

    try:
        parsed = yaml.safe_load(result)
    except Exception as e:
        logger.warning(f"[runtime] LLM 诊断结果解析失败: {e}\n原始: {result[:300]}")
        return False, None, None

    if not isinstance(parsed, dict):
        return False, None, None

    is_violation = bool(parsed.get("is_violation", False))
    if not is_violation:
        logger.info(f"[runtime] LLM 判定: 非语义违规 — {parsed.get('diagnosis', '')}")
        return False, None, None

    logger.info(f"[runtime] LLM 判定违规: {parsed.get('diagnosis', '')}")


    fix_patch = parsed.get("fix_patch", {})
    fixed_cr: Optional[dict] = None
    if fix_patch and isinstance(fix_patch, dict):
        from core.patch import _apply_patch_to_cr
        try:
            fixed_cr = _apply_patch_to_cr(mutated_cr, fix_patch)
        except Exception as e:
            logger.warning(f"[runtime] fix_patch 应用失败: {e}")


    c_raw = parsed.get("constraint", {})
    constraint: Optional[dict] = None
    if c_raw and isinstance(c_raw, dict) and c_raw.get("rule"):
        constraint = {
            "type": c_raw.get("type", "conditional"),
            "severity": c_raw.get("severity", "error"),
            "fields": c_raw.get("fields", []),
            "rule": c_raw.get("rule", ""),
            "check": c_raw.get("check", ""),
            "fix_hint": c_raw.get("fix_hint", ""),
            "source": "runtime",
            "divergence_branch": branch_index,
        }

    return True, fixed_cr, constraint


def _runtime_constraint_path(profile_dir: str) -> str:
    return os.path.join(profile_dir, _RUNTIME_CONSTRAINT_FILE)


def load_runtime_constraints(profile_dir: str) -> dict:
    """Load runtime_constraints.json; returns {\"constraints\": [...]} or empty."""
    path = _runtime_constraint_path(profile_dir)
    if not os.path.exists(path):
        return {"constraints": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(
            f"[runtime] 已加载 {path}，共 {len(data.get('constraints', []))} 条运行时约束"
        )
        return data
    except Exception as e:
        logger.warning(f"[runtime] 加载 runtime_constraints.json 失败: {e}")
        return {"constraints": []}


def save_runtime_constraint(profile_dir: str, constraint: dict) -> None:
    """Append a new constraint to runtime_constraints.json (dedup by rule)."""
    path = _runtime_constraint_path(profile_dir)
    data = load_runtime_constraints(profile_dir)
    existing = data.get("constraints", [])


    rule = constraint.get("rule", "")
    if any(c.get("rule") == rule for c in existing):
        logger.info(f"[runtime] 约束已存在，跳过: {rule[:80]}")
        return


    cid = f"RC{len(existing) + 1:03d}"
    constraint = {"id": cid, **constraint}
    existing.append(constraint)
    data["constraints"] = existing

    os.makedirs(profile_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"[runtime] 新约束已保存 [{cid}]: {rule[:80]}")