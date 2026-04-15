from typing import List, Optional

import yaml

from core.cr_utils import _fmt_current_value
from crd.schema import (
    _extract_crd_required_fields,
    _extract_crd_schema_for_fields,
    _extract_required_siblings,
)


def _build_branch_flip_prompt(
    branch_meta: dict,
    current_value: Optional[bool],
    target_value: bool,
    source_context: str,
    related_fields: List[dict],
    base_cr_yaml: str,
    crd_file: str,
    cr_kind: str,
    combo_targets: List[dict] = None,
    error_feedback: str = "",
    include_source_code: bool = False,
    constraints_txt: str = "",
) -> str:
    """构建 LLM prompt，要求输出 patch（set/delete）让目标 branch 翻转。"""


    cond_ = branch_meta.get("Fmt") or branch_meta.get("Raw", "")


    tgt_str = "True" if target_value else "False"


    expr_lines = []
    for expr in branch_meta.get("Expressions") or []:
        e_fmt = expr.get("fmt") or expr.get("raw", "")
        variables = expr.get("variables", [])
        if variables:
            var_parts = []
            for var in variables:
                v_fmt = var.get("fmt") or var.get("raw", "")
                v_kind = var.get("kind", "")
                cr_flds = [
                    rf["field_path"]
                    for rf in related_fields
                    if v_fmt in rf.get("variable_fmts", [])
                ]
                if cr_flds:
                    var_parts.append(
                        f"    - variable `{v_fmt}` ({v_kind}) → CR field(s): {', '.join(cr_flds)}"
                    )
                else:
                    var_parts.append(
                        f"    - variable `{v_fmt}` ({v_kind}) (no direct CR mapping)"
                    )
            expr_lines.append(
                f"  Expr `{e_fmt}` (op={expr.get('op', '')!r}):\n"
                + "\n".join(var_parts)
            )
        else:
            cr_flds = [
                rf["field_path"]
                for rf in related_fields
                if e_fmt in rf.get("variable_fmts", [])
            ]
            if cr_flds:
                expr_lines.append(
                    f"  Expr `{e_fmt}` (whole-expression, op={expr.get('op', '')!r})"
                    f" → CR field(s): {', '.join(cr_flds)}"
                )
            else:
                expr_lines.append(
                    f"  Expr `{e_fmt}` (whole-expression, op={expr.get('op', '')!r})"
                    f" — correlate via condition semantics"
                )
    expr_section = (
        "\n## Branch Expressions & Variables\n" + "\n".join(expr_lines)
        if expr_lines
        else ""
    )


    hint_lines = _derive_value_hints(branch_meta.get("Expressions") or [], target_value)
    hints_section = (
        "\n## Value Direction Hints (to achieve target={})\n".format(tgt_str)
        + "\n".join(hint_lines)
        if hint_lines
        else ""
    )


    field_paths_for_schema = (
        [f["field_path"] for f in related_fields] if related_fields else []
    )
    crd_schema_snippet = _extract_crd_schema_for_fields(
        crd_file, cr_kind, field_paths_for_schema
    )
    if related_fields:
        fields_txt = "\n".join(
            f"  - {f['field_path']} (type: {f.get('field_type', '?')})"
            for f in related_fields
        )
        target_fields_hint = ", ".join(f["field_path"] for f in related_fields[:5])
    else:
        fields_txt = "  (no known related fields — reason from the condition)"
        target_fields_hint = "any spec field that influences the condition"


    required_txt = constraints_txt if constraints_txt else ""


    combo_txt = ""
    if combo_targets:
        parts = []
        for ct in combo_targets:
            bm = ct["branch_meta"]
            tv = ct["target_value"]
            parts.append(
                f"  - BranchIndex={bm.get('BranchIndex', '?')} "
                f"condition: `{bm.get('Fmt') or bm.get('Raw', '?')}` "
                f"→ target: {'True' if tv else 'False'}"
            )
        combo_txt = (
            "\n## Additional Branches to Flip Simultaneously\n"
            + "\n".join(parts)
            + "\n"
        )


    source_section = ""
    if include_source_code:
        source_section = f"""
## Source Code Context (>>> marks the target branch)
```go
{source_context if source_context else "(unavailable)"}
```
"""

    error_txt = ""
    if error_feedback:
        error_txt = f"""
## Previous Attempt Failed
```
{error_feedback}
```
You MUST change your approach. Do NOT repeat the same field values.
"""

    return f"""You are an expert Kubernetes operator test engineer.
Your goal: determine which CR field(s) to change so that the condition below evaluates to **{tgt_str}**.

## Target Branch Condition
`{cond_}`
{expr_section}
{hints_section}
## CR Fields Known to Affect This Branch
{fields_txt}
{combo_txt}
## CRD Schema for Related Fields
```yaml
{crd_schema_snippet if crd_schema_snippet else "(schema not available — refer to condition semantics)"}
```
{required_txt}
## Current CR (base)
```yaml
{base_cr_yaml}
```
{source_section}{error_txt}
## Instructions
1. Analyse the condition `{cond_}` and the variable→CR field mappings above.
2. Decide which field(s) to change (primarily: {target_fields_hint}).
3. Output ONLY a YAML patch with two keys — `set` and `delete` — nothing else.
   - `set`: a mapping of dot-notation field paths to their new values.
   - `delete`: a list of dot-notation field paths to remove (usually empty).
4. Do NOT output the full CR. Do NOT include markdown fences.
5. Fields in the Required Fields list MUST NOT appear under `delete` and must keep valid values under `set`.
6. CRITICAL: Only set fields that are explicitly defined in the CRD schema above.
   Do NOT invent generic Kubernetes fields (e.g. ephemeralContainers, initContainers
   sub-paths, volumeDevices, etc.) unless they appear in the CRD schema.

Example output format:
set:
  spec.size: 3
  spec.config.num_tokens: 256
delete: []
"""


def _related_fields_for_branch(bi: int, field_relations: dict) -> List[dict]:
    """返回 field_relations 中与 branch bi 相关的字段列表（含变量映射信息）。"""
    result = []
    for fp, fdata in field_relations.items():
        if bi in (fdata.get("branch_indices") or []):
            vm = fdata.get("variable_mappings", {}).get(str(bi), {})
            var_fmts = [
                vinfo.get("variable_fmt", "")
                for vinfo in vm.values()
                if vinfo.get("variable_fmt")
            ]

            if not var_fmts:
                expr_fmts = fdata.get("expression_fmts", {}).get(str(bi), [])
                var_fmts = expr_fmts
            result.append(
                {
                    "field_path": fp,
                    "field_type": fdata.get("field_type", ""),
                    "variable_fmts": var_fmts,
                }
            )
    return result


def _derive_value_hints(expressions: list, target_value: bool) -> list:
    """Return plain-English constraint hints for each expression given the target bool.

    For each expression we:
      1. Determine the effective target after applying op-level negation ('!' prefix).
      2. Detect the comparison operator in the raw/fmt text.
      3. Emit a concrete constraint string.

    Returns a list of strings, one per expression, empty strings when no hint can be derived.
    """
    import re as _re


    _HINT: dict = {
        (">", True): "%L must be > %R",
        (">", False): "%L must be ≤ %R",
        (">=", True): "%L must be ≥ %R",
        (">=", False): "%L must be < %R",
        ("<", True): "%L must be < %R",
        ("<", False): "%L must be ≥ %R",
        ("<=", True): "%L must be ≤ %R",
        ("<=", False): "%L must be > %R",
        ("==", True): "%L must equal %R",
        ("==", False): "%L must NOT equal %R",
        ("!=", True): "%L must NOT equal %R (i.e. must differ from %R)",
        ("!=", False): "%L must equal %R",

        ("!= nil", True): "%L must be non-nil (set a value)",
        ("!= nil", False): "%L must be nil (omit the field)",
        ("== nil", True): "%L must be nil (omit the field)",
        ("== nil", False): "%L must be non-nil (set a value)",

        ("len > 0", True): "%L must be non-empty (length > 0)",
        ("len > 0", False): "%L must be empty (length == 0)",
        ("len == 0", True): "%L must be empty",
        ("len == 0", False): "%L must be non-empty",
        ("len >= 1", True): "%L must have at least one element",
        ("len >= 1", False): "%L must be empty",

        ("bool_true", True): "%L must be true",
        ("bool_true", False): "%L must be false",
        ("bool_false", True): "%L must be false",
        ("bool_false", False): "%L must be true (negate the condition)",
    }

    hints = []
    for expr in expressions:
        raw = (expr.get("fmt") or expr.get("raw", "")).strip()
        op_prefix = (expr.get("op") or "").strip()


        negated = op_prefix == "!"
        eff_target = (not target_value) if negated else target_value

        hint = ""

        m = _re.match(r"^(.+?)\s*(!=|==)\s*nil$", raw)
        if m:
            lhs = m.group(1).strip()
            op_tok = m.group(2) + " nil"
            tmpl = _HINT.get((op_tok, eff_target), "")
            if tmpl:
                hint = tmpl.replace("%L", f"`{lhs}`").replace("%R", "nil")

        if not hint:
            m = _re.match(r"^len\((.+?)\)\s*(>=|>|==|!=|<=|<)\s*(\d+)$", raw)
            if m:
                inner = m.group(1).strip()
                op_tok = m.group(2)
                rhs = m.group(3)

                if op_tok == ">" and rhs == "0":
                    tmpl = _HINT.get(("len > 0", eff_target), "")
                elif op_tok == ">=" and rhs == "1":
                    tmpl = _HINT.get(("len >= 1", eff_target), "")
                elif op_tok == "==" and rhs == "0":
                    tmpl = _HINT.get(("len == 0", eff_target), "")
                else:
                    tmpl = _HINT.get((op_tok, eff_target), "")
                if tmpl:
                    hint = tmpl.replace("%L", f"len(`{inner}`)").replace("%R", rhs)

        if not hint:
            m = _re.match(r"^(.+?)\s*(>=|<=|!=|==|>|<)\s*(.+)$", raw)
            if m:
                lhs = m.group(1).strip()
                op_tok = m.group(2)
                rhs = m.group(3).strip()
                tmpl = _HINT.get((op_tok, eff_target), "")
                if tmpl:
                    hint = tmpl.replace("%L", f"`{lhs}`").replace("%R", f"`{rhs}`")

        if not hint:

            if not _re.search(r"[><=!]", raw) and not raw.startswith("len("):

                key = "bool_true"
                tmpl = _HINT.get((key, eff_target), "")
                if tmpl:
                    hint = tmpl.replace("%L", f"`{raw}`").replace("%R", "")

        if hint:
            neg_note = f" [negated by '{op_prefix}']" if negated else ""
            hints.append(f"  `{raw}`{neg_note} → {hint}")

    return hints


def _build_phase1_prompt(
    base_cr_yaml: str,
    field_path: str,
    crd_file: str,
    cr_kind: str,
    error_feedback: str = "",
    base_cr: Optional[dict] = None,
    constraints_txt: str = "",
) -> str:
    """为 Phase 1 单字段变异构建 LLM prompt，返回 patch 格式。"""
    schema_snippet = _extract_crd_schema_for_fields(crd_file, cr_kind, [field_path])
    all_required = _extract_crd_required_fields(crd_file, cr_kind)
    sibling_required = _extract_required_siblings(all_required, field_path)
    sibling_txt = ""
    if sibling_required:
        req_list = "\n".join(f"  - {p}" for p in sibling_required)
        sibling_txt = f"\n## Required Sibling Fields (these fields share the same object scope as the target and MUST remain present with valid values)\n{req_list}\n"
    error_txt = ""
    if error_feedback:
        error_txt = f"""\n## Previous Attempt Failed\n```\n{error_feedback}\n```\nYou MUST change your approach. Do NOT repeat the same value.\n"""

    current_val_str = (
        _fmt_current_value(base_cr, field_path) if base_cr is not None else ""
    )
    current_val_txt = ""
    if current_val_str:
        current_val_txt = f"\n## CRITICAL: Current Value of Target Field\nThe field `{field_path}` currently has this value:\n  {current_val_str}\nYou MUST output a DIFFERENT value. Do NOT use `{current_val_str}` as the new value.\n"
    constraints_section = f"\n{constraints_txt}" if constraints_txt else ""
    return f"""You are an expert Kubernetes operator test engineer.
Your task: change ONLY the field `{field_path}` in the CR to a different VALID value.

## Target Field
Path: {field_path}

## CRD Schema for This Field
```yaml
{schema_snippet if schema_snippet else "(schema not available)"}
```

## Current CR (for context only — do NOT reproduce it)
```yaml
{base_cr_yaml}
```
{current_val_txt}{sibling_txt}{constraints_section}{error_txt}## Instructions
1. Choose a new valid value for `{field_path}` that is DIFFERENT from its current value.
2. Output ONLY a YAML patch with two keys — `set` and `delete` — nothing else.
   - `set`: a mapping of dot-notation field paths to their new values (at minimum `{field_path}`).
   - `delete`: a list of dot-notation field paths to remove (usually empty).
3. Do NOT output the full CR. Do NOT include markdown fences.
4. Fields in the Required Sibling Fields list MUST NOT appear under `delete`.
5. Respect ALL constraints listed under Field Constraints above.
6. CRITICAL: Only set fields that are explicitly defined in the CRD schema above.
   Do NOT invent generic Kubernetes fields (e.g. ephemeralContainers, initContainers
   sub-paths, volumeDevices, etc.) unless they appear in the CRD schema.

Example output format:
set:
  {field_path}: <new_value>
delete: []
"""


def _build_explore_add_prompt(
    base_cr_yaml: str,
    field_path: str,
    crd_file: str,
    cr_kind: str,
    error_feedback: str = "",
    base_cr: Optional[dict] = None,
    constraints_txt: str = "",
) -> str:
    """Build LLM prompt to ADD a field absent from the CR; returns patch format."""
    schema_snippet = _extract_crd_schema_for_fields(crd_file, cr_kind, [field_path])
    all_required = _extract_crd_required_fields(crd_file, cr_kind)
    sibling_required = _extract_required_siblings(all_required, field_path)
    sibling_txt = ""
    if sibling_required:
        req_list = "\n".join(f"  - {p}" for p in sibling_required)
        sibling_txt = f"\n## Required Sibling Fields (these fields share the same object scope as the target and MUST remain present with valid values)\n{req_list}\n"
    error_txt = ""
    if error_feedback:
        error_txt = f"""\n## Previous Attempt Failed\n```\n{error_feedback}\n```\nYou MUST change your approach. Do NOT repeat the same value.\n"""

    current_val_str = (
        _fmt_current_value(base_cr, field_path) if base_cr is not None else ""
    )
    current_val_txt = ""
    if current_val_str:
        current_val_txt = f"\n## Note: Field Already Present\nThe field `{field_path}` already has value: {current_val_str}\nYou MUST set it to a DIFFERENT value.\n"
    constraints_section = f"\n{constraints_txt}" if constraints_txt else ""
    return f"""You are an expert Kubernetes operator test engineer.
Your task: ADD the field `{field_path}` (currently absent) to the CR with a valid non-trivial value.

## Target Field to ADD
Path: {field_path}

## CRD Schema for This Field
```yaml
{schema_snippet if schema_snippet else "(schema not available)"}
```

## Current CR (for context only — do NOT reproduce it)
```yaml
{base_cr_yaml}
```
{current_val_txt}{sibling_txt}{constraints_section}{error_txt}## Instructions
1. Choose a valid, non-trivial value for `{field_path}` and add it to the CR.
2. Output ONLY a YAML patch with two keys — `set` and `delete` — nothing else.
   - `set`: must include `{field_path}` with a suitable non-trivial value.
   - `delete`: a list of dot-notation field paths to remove (usually empty).
3. Do NOT output the full CR. Do NOT include markdown fences.
4. Fields in the Required Sibling Fields list MUST NOT appear under `delete`.
5. Respect ALL constraints listed under Field Constraints above.
6. CRITICAL: Only set fields that are explicitly defined in the CRD schema above.
   Do NOT invent generic Kubernetes fields (e.g. ephemeralContainers, initContainers
   sub-paths, volumeDevices, etc.) unless they appear in the CRD schema.

Example output format:
set:
  {field_path}: <new_value>
delete: []
"""


def _build_test_plan_prompt(
    field_path: str,
    crd_file: str,
    cr_kind: str,
    base_cr: dict,
    field_present: bool,
    field_optional: bool,
) -> str:
    """Build a prompt asking the LLM to generate a chained test plan for a CR field.

    The plan is a YAML list of steps.  Each step either sets the field to a new
    representative value, or removes it (if optional).  Steps are ordered so that
    each one can use the previous step's result as its baseline (chained comparison).

    Returns the prompt string.
    """
    schema_snippet = _extract_crd_schema_for_fields(crd_file, cr_kind, [field_path])
    all_required = _extract_crd_required_fields(crd_file, cr_kind)
    sibling_required = _extract_required_siblings(all_required, field_path)
    required_txt = ""
    if sibling_required:
        req_list = "\n".join(f"  - {p}" for p in sibling_required)
        required_txt = (
            f"\n## Required Sibling Fields\n"
            f"These fields share the same object/array-element scope as the target.\n"
            f"When you output `to` values that affect the parent object, ALL of these\n"
            f"must remain present with valid values in the resulting CR:\n{req_list}\n"
        )
    current_val_str = _fmt_current_value(base_cr, field_path)
    current_val_txt = (
        f"The field is currently set to: `{current_val_str}`"
        if field_present and current_val_str
        else "The field is currently ABSENT from the CR."
    )
    remove_step_txt = (
        "\n- You MAY include one step with `remove: true` to test the absent case "
        "(only if the field is optional in the CRD)."
        if field_optional
        else "\n- Do NOT include a remove step — this field is REQUIRED by the CRD."
    )
    cr_yaml_snippet = yaml.dump(base_cr, default_flow_style=False)

    return f"""You are an expert Kubernetes operator test engineer designing a mutation test plan.

## Goal
Generate a short, representative test plan for the CR field `{field_path}`.
The plan will be executed sequentially: each step uses the PREVIOUS step's result as its
starting point, enabling chained before/after comparison to detect how code branches respond.

## Field Information
- Path:     {field_path}
- CRD Kind: {cr_kind}
- {current_val_txt}

## CRD Schema for This Field
```yaml
{schema_snippet if schema_snippet else "(schema not available)"}
```

## Current CR (context only)
```yaml
{cr_yaml_snippet}
```
{required_txt}
## Requirements
1. Generate 3-5 steps total (fewer is better — quality over quantity).
2. Each `to` value must be VALID according to the CRD schema.
3. Values must be REPRESENTATIVE — cover meaningful semantic boundaries
   (e.g. zero vs positive, small vs large, default vs non-default, edge cases).
4. Steps must be CHAINED: step N starts from the result of step N-1.
   So consecutive steps should use DIFFERENT values from each other.
5. The first step's `from` is the current value shown above.{remove_step_txt}
6. ONLY set the target field `{field_path}`. Do NOT touch sibling fields.

## Output Format
Output ONLY valid YAML. No markdown fences. No extra keys. Example:

steps:
  - to: 50
    rationale: "lower bound — tests minimal weight"
  - to: 100
    rationale: "original value — verifies restore"
  - to: 0
    rationale: "edge case — zero weight"
  - remove: true
    rationale: "tests absent case"
"""


def _build_diverse_cr_prompt(
    base_cr_yaml: str,
    crd_file: str,
    cr_kind: str,
    uncovered_count: int = 0,
    constraints_txt: str = "",
) -> str:
    """Build a prompt that asks LLM to generate a structurally diverse but valid CR.

    Used when testplan is stuck (no new branch coverage for N consecutive rounds).
    The LLM should output a full CR YAML (not a patch).
    """
    all_required = _extract_crd_required_fields(crd_file, cr_kind)
    req_list = (
        "\n".join(f"  - {p}" for p in sorted(all_required))
        if all_required
        else "  (none — all spec fields are optional)"
    )
    schema_snippet = _extract_crd_schema_for_fields(crd_file, cr_kind, [])

    return f"""You are a Kubernetes operator test engineer.
Your task: generate a NEW, DIVERSE but VALID CR that is structurally different from the current base CR.
The new CR should activate different code paths in the operator by using a different combination of optional fields and values.

## Current Base CR (do NOT reproduce this — make it meaningfully different)
```yaml
{base_cr_yaml}
```

## CRD Schema (reference for valid fields and value types)
```yaml
{schema_snippet if schema_snippet else "(schema not available — infer from the current CR structure)"}
```

## Required Fields (MUST be present with valid values in the new CR)
{req_list}
{constraints_txt}
## Context
There are currently {uncovered_count} uncovered branches remaining.
The current base CR has been tested extensively but has not revealed new branches.
A structurally different CR is needed to exercise different operator code paths.

## Instructions
1. Output a COMPLETE, valid CR YAML for kind `{cr_kind}`.
2. Keep the same `metadata.name` and `metadata.namespace` as the base CR.
3. Make `spec` meaningfully DIFFERENT:
   - Enable optional features that are currently absent.
   - Disable or change features that are currently present.
   - Use different values for existing fields (different sizes, versions, resource limits, etc.).
4. All required fields MUST remain present with valid values.
5. Only use fields explicitly defined in the CRD schema above.
   Do NOT invent generic Kubernetes fields unless they appear in the schema.
6. Output ONLY the full CR YAML. No markdown fences. No explanations.
"""


def _build_direct_value_prompt(
    field_path: str,
    crd_file: str,
    cr_kind: str,
    base_cr: dict,
    field_present: bool,
    error_feedback: str = "",
    constraints_txt: str = "",
) -> str:
    """Build a prompt asking LLM for exactly ONE valid alternative value for a field.

    The goal is to produce a before/after pair where:
    - before = base_cr (already applied, trace collected)
    - after  = base_cr with field_path set to a DIFFERENT valid value

    Both CRs must be accepted by the API server.  The LLM outputs a minimal patch
    (set/delete keys only) — identical to _build_phase1_prompt but with a simpler
    single-value objective and no chaining / multi-step framing.
    """
    schema_snippet = _extract_crd_schema_for_fields(crd_file, cr_kind, [field_path])
    all_required = _extract_crd_required_fields(crd_file, cr_kind)
    sibling_required = _extract_required_siblings(all_required, field_path)

    current_val_str = _fmt_current_value(base_cr, field_path)
    if field_present and current_val_str:
        field_status = f"Currently set to: `{current_val_str}`"
        action = (
            f"Choose ONE valid alternative value for `{field_path}` that is DIFFERENT "
            f"from the current value `{current_val_str}`, then output a minimal patch."
        )
    else:
        field_status = "Currently ABSENT from the CR."
        action = (
            f"Add `{field_path}` with ONE valid value, then output a minimal patch."
        )

    sibling_txt = ""
    if sibling_required:
        req_list = "\n".join(f"  - {p}" for p in sibling_required)
        sibling_txt = (
            f"\n## Required Sibling Fields\n"
            f"These fields MUST remain present with valid values:\n{req_list}\n"
        )

    error_txt = (
        f"\n## Previous Attempt Error\n{error_feedback}\nPlease fix this.\n"
        if error_feedback
        else ""
    )

    cr_yaml = yaml.dump(base_cr, default_flow_style=False)

    return f"""You are a Kubernetes CR mutation expert.

## Objective
{action}

## Field
- Path:    {field_path}
- Kind:    {cr_kind}
- Status:  {field_status}

## CRD Schema (for this field)
```yaml
{schema_snippet if schema_snippet else "(not available)"}
```

## Current CR
```yaml
{cr_yaml}
```
{sibling_txt}{error_txt}{constraints_txt}
## Rules
1. Output ONLY a patch with `set:` and `delete:` keys. No other YAML keys.
2. The mutated CR MUST be valid and accepted by the Kubernetes API server.
3. Change ONLY `{field_path}` (plus any required sibling fields that need adjustment).
4. Do NOT change unrelated fields.
5. The new value MUST differ from the current value.

## Output Format
set:
  {field_path}: <new_value>
delete: []
"""


def _get_baseline_cr_for_branch(
    branch_baseline_crs: dict,
    bi: int,
    default_cr_yaml: str,
) -> str:
    """Return the best-fit baseline CR YAML for a given branch index.
    Falls back to default_cr_yaml if no specific entry exists.
    """
    return branch_baseline_crs.get(str(bi), default_cr_yaml)


if __name__ == "__main__":
    import json

    with open("/mnt/d/instrument/CassOp/field_relations.json", "r") as fp:
        data = json.load(fp)

    print(_related_fields_for_branch(6, data))