

import json
import logging
import os
import re
from typing import List, Optional

import yaml

logger = logging.getLogger(__name__)

_PREFLIGHT_PROMPT_TEMPLATE = """\
你是一个 Kubernetes CRD 专家，负责从 CRD 定义中提取字段约束关系，用于指导大模型对 CR 进行变异操作时的合法性校验。

## 任务目标
分析以下 CRD 的 spec 字段定义，**尽可能完整地**提取所有字段约束。约束来源包括：
1. **schema 层**：`required`、`enum`、`minimum`/`maximum`、`pattern`、`minLength`、`x-kubernetes-*` 等
2. **description 层**：字段 description 中明确描述的存在性、互斥、条件依赖、优先级、废弃关系

## CRD 内容
<crd>
{CRD_YAML}
</crd>

## 输出格式要求
只输出 JSON，不要输出任何其他内容：

{{
  "constraints": [
    {{
      "id": "C001",
      "type": "<约束类型>",
      "severity": "error | warning",
      "fields": ["字段路径1", "字段路径2"],
      "rule": "一句话精确描述约束内容",
      "check": "伪代码或条件表达式，描述如何判断是否违反",
      "violation_example": "违反此约束的示例片段（YAML格式）",
      "fix_hint": "违反时应如何修正"
    }}
  ]
}}

## 约束类型枚举
只使用以下类型，不得自创：
- required          字段必须存在且非空
- mutual_exclusion  字段组中最多只能设置一个
- co_required       字段组必须同时存在（要么都有，要么都没有）
- conditional       当条件字段满足某值/存在时，目标字段的存在性或取值有约束
- enum              字段取值必须在指定集合内
- format            字段取值须满足特定格式（正则/类型）
- range             字段取值须在数值范围内
- precedence        多字段冲突时的优先级规则（severity=warning）
- immutable         字段一旦设置不可变更（severity=warning）
- deprecation       字段已废弃（severity=warning）
- best_practice     字段组合在运维/架构层面有强烈建议关系，违反不直接
                    导致 CR 不合法但可能引发集群问题（severity=warning）

## 约束提取规则

### 数量与粒度
1. **不合并不同类型**：同一字段若同时有 required、enum、format、range 约束，每种类型各生成一条，不合并。
   - 例如 `spec.serverType` 同时有 required 和 enum，输出两条：C_required 和 C_enum。
2. **数组字段**：对 `spec.foo[*].bar` 格式的必填/格式约束，使用 `FOR_EACH` 伪代码逐元素描述。
3. **deprecation / immutable**：凡 description 中出现 "deprecated"、"immutable"、"cannot be changed" 等字样，均提取，不限变异意图。
4. **description 语义约束识别模式**：

扫描每个字段的 description，按以下模式分类提取：

**→ conditional**（条件依赖）
  触发词：when、if、only when、requires、must be set when

**→ mutual_exclusion**（互斥）
  触发词：mutually exclusive、cannot be set together、only one of

**→ precedence**（优先级）
  触发词：takes precedence、overrides、will be used exclusively

**→ immutable**（不可变）
  触发词：immutable、cannot be updated、cannot be changed

**→ deprecation**（废弃）
  触发词：deprecated、DEPRECATED

**→ best_practice**（架构建议）
  触发词：
  - should match、should be、is recommended、it is recommended
  - cannot easily be changed（说明字段变更代价极高）
  - setting to X might cause（说明取值有潜在风险）
  - the number of X should match Y（数量协调关系）
  - 描述字段间规模/数量的对应建议

  提取要求：
  - fields 列出所有被关联的字段路径
  - rule 用一句话描述建议关系及违反后果
  - check 使用 RECOMMEND(...) 原语
  - severity 固定为 warning

### severity 判定
- schema 层硬约束（required、enum、minimum、pattern 等）→ severity=error
- description 层语义约束、precedence、deprecation、immutable → severity=warning

### check 伪代码约定
- EXISTS(field)              字段存在且非空
- VALUE(field)               字段的值
- IN(value, set)             值在集合中
- MATCH(value, regex)        值匹配正则
- RANGE(value, min, max)     值在数值范围内（min/max 用 _ 表示无限制）
- LENGTH(value) >= N         字符串最小长度
- FOR_EACH item IN field: <expr>  对数组每个元素的约束
- IF / THEN / AND / OR / NOT 逻辑运算符
- RECOMMEND(condition)       架构建议，condition 为 false 时输出 warning，不阻断变异
"""


def _get_constraint_file_path(profile_dir: str) -> str:
    """返回 constraint.json 的预期路径。"""
    return os.path.join(profile_dir, "constraint.json")


def load_constraints(profile_dir: str) -> Optional[dict]:
    """从 profile 目录加载 constraint.json，不存在时返回 None。"""
    path = _get_constraint_file_path(profile_dir)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(
            f"[constraints] 已加载 {path}，共 {len(data.get('constraints', []))} 条约束"
        )
        return data
    except Exception as e:
        logger.warning(f"[constraints] 加载 constraint.json 失败: {e}")
        return None


_K8S_BOILERPLATE_FIELDS = {
    "podTemplateSpec",
    "cassandraDataVolumeClaimSpec",
    "pvcSpec",
    "volumeSource",
    "dataVolumeClaimSpec",
}


def _collapse_boilerplate(node: dict, field_name: str) -> dict:
    """Replace a boilerplate field's properties with a one-line stub, keeping
    description, type, and required so the LLM still knows the field exists."""
    stub: dict = {}
    for keep in (
        "type",
        "description",
        "required",
        "x-kubernetes-preserve-unknown-fields",
    ):
        if keep in node:
            stub[keep] = node[keep]
    stub["properties"] = {
        "_note": {
            "type": "string",
            "description": f"(K8s native schema omitted — {field_name})",
        }
    }
    return stub


def _prune_spec_properties(spec_props: dict) -> dict:
    """Recursively walk spec properties, collapsing known boilerplate fields."""
    import copy

    result: dict = {}
    for name, schema in spec_props.items():
        if name in _K8S_BOILERPLATE_FIELDS:
            result[name] = _collapse_boilerplate(schema, name)
            continue
        node = copy.deepcopy(schema)

        if "properties" in node and isinstance(node["properties"], dict):
            node["properties"] = _prune_spec_properties(node["properties"])

        if "items" in node and isinstance(node["items"], dict):
            items = node["items"]
            if "properties" in items and isinstance(items["properties"], dict):
                items["properties"] = _prune_spec_properties(items["properties"])
        result[name] = node
    return result


def _extract_crd_yaml_from_context(context_file: str) -> str:
    """从 context.json 提取 CRD spec properties schema 并转为 YAML 字符串。

    只向 LLM 发送 spec 的 properties 层（operator 自定义字段），丢弃
    status / metadata / additionalPrinterColumns / subresources 等无关内容，
    并将 podTemplateSpec / PVC 等 K8s 原生子 schema 折叠为单行 stub。
    """
    with open(context_file, "r", encoding="utf-8") as f:
        ctx = json.load(f)
    crd_body = ctx.get("crd", {}).get("body", {})
    if not crd_body:
        raise ValueError("context.json 中未找到 crd.body")


    kind = crd_body.get("spec", {}).get("names", {}).get("kind", "CR")


    versions = crd_body.get("spec", {}).get("versions", [])
    spec_schema: dict = {}
    for ver in versions:
        root = ver.get("schema", {}).get("openAPIV3Schema", {})
        props = root.get("properties", {}).get("spec", {})
        if props:
            spec_schema = props
            break

    if not spec_schema:

        return yaml.dump(crd_body, allow_unicode=True, sort_keys=False)

    spec_props = spec_schema.get("properties", {})
    pruned_props = _prune_spec_properties(spec_props)


    minimal = {
        "kind": kind,
        "spec": {
            "description": spec_schema.get("description", ""),
            "required": spec_schema.get("required", []),
            "properties": pruned_props,
        },
    }
    return yaml.dump(minimal, allow_unicode=True, sort_keys=False)


def run_preflight(context_file: str, profile_dir: str) -> dict:
    """生成并持久化 constraint.json。

    从 context_file 读取 CRD，调用 LLM 生成约束，写入 profile_dir/constraint.json。
    返回生成的约束 dict。
    """
    from llm.client import _call_llm_for_branch_flip

    constraint_path = _get_constraint_file_path(profile_dir)
    logger.info(f"[preflight] 开始生成约束: {constraint_path}")

    if not os.path.exists(context_file):
        raise FileNotFoundError(f"context.json 不存在: {context_file}")

    crd_yaml = _extract_crd_yaml_from_context(context_file)
    prompt = _PREFLIGHT_PROMPT_TEMPLATE.format(CRD_YAML=crd_yaml)
    with open(os.path.join(profile_dir, "prompt.txt"), "w") as fp:
        fp.write(prompt)
    logger.info("[preflight] 调用 LLM 生成约束...")
    action, result = _call_llm_for_branch_flip(prompt, max_tokens=8192)
    if action == "error":
        raise RuntimeError(f"LLM 调用失败: {result}")


    raw = result.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        constraints_data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"LLM 返回的约束 JSON 解析失败: {e}\n原始内容:\n{raw[:500]}")

    if "constraints" not in constraints_data:
        constraints_data = {
            "constraints": constraints_data
            if isinstance(constraints_data, list)
            else []
        }

    os.makedirs(profile_dir, exist_ok=True)
    with open(constraint_path, "w", encoding="utf-8") as f:
        json.dump(constraints_data, f, ensure_ascii=False, indent=2)

    n = len(constraints_data.get("constraints", []))
    logger.info(f"[preflight] 约束已保存: {constraint_path}，共 {n} 条")
    return constraints_data


def ensure_constraints(context_file: str, profile_dir: str) -> dict:
    """确保 constraint.json 存在；不存在则调用 preflight 生成。

    同时将 runtime_constraints.json 合并进返回结果（不修改文件本身）。
    返回合并后的约束 dict（可能为空 dict 如果出错）。
    """
    existing = load_constraints(profile_dir)
    if existing is None:
        logger.info("[constraints] constraint.json 不存在，自动运行 preflight...")
        try:
            existing = run_preflight(context_file, profile_dir)
        except Exception as e:
            logger.warning(f"[constraints] preflight 失败，继续但无约束信息: {e}")
            existing = {"constraints": []}


    try:
        from llm.runtime_constraints import load_runtime_constraints

        rt = load_runtime_constraints(profile_dir)
        rt_list = rt.get("constraints", [])
        if rt_list:
            merged = list(existing.get("constraints", [])) + rt_list
            existing = {**existing, "constraints": merged}
            logger.info(
                f"[constraints] 合并运行时约束 {len(rt_list)} 条，共 {len(merged)} 条"
            )
    except Exception as e:
        logger.debug(f"[constraints] 合并运行时约束失败（忽略）: {e}")

    return existing


def filter_constraints(constraints_data: dict, field_paths: List[str]) -> List[dict]:
    """筛选与给定字段路径相关的约束。

    字段路径匹配规则：
    - 约束的 fields 列表中有任意字段与 field_paths 中的字段路径有前缀重叠，则纳入
    - 约束的 fields 为空时始终纳入（全局约束）
    """
    if not constraints_data:
        return []
    all_constraints: List[dict] = constraints_data.get("constraints", [])
    if not field_paths:
        return all_constraints


    def _norm(p: str) -> str:
        return re.sub(r"\[\*?\d*\]", "", p).strip(".")

    norm_targets = {_norm(fp) for fp in field_paths}

    result = []
    for c in all_constraints:
        c_fields: List[str] = c.get("fields", [])
        if not c_fields:
            result.append(c)
            continue
        for cf in c_fields:
            cf_norm = _norm(cf)
            for nt in norm_targets:

                if (
                    cf_norm == nt
                    or cf_norm.startswith(nt + ".")
                    or nt.startswith(cf_norm + ".")
                ):
                    result.append(c)
                    break
            else:
                continue
            break

    return result


def format_constraints_section(constraints: List[dict]) -> str:
    """将约束列表格式化为 prompt 中的文本段落。

    只输出 error severity 的约束的关键信息（rule + fix_hint），
    warning 级别也包含但标注为 warning。
    返回空字符串表示无相关约束。
    """
    if not constraints:
        return ""

    lines = ["## Field Constraints (MUST respect when mutating)"]
    for c in constraints:
        severity = c.get("severity", "error")
        cid = c.get("id", "?")
        rule = c.get("rule", "")
        fix = c.get("fix_hint", "")
        fields = ", ".join(f"`{f}`" for f in c.get("fields", []))
        severity_tag = "[ERROR]" if severity == "error" else "[WARNING]"
        lines.append(f"- {severity_tag} {cid}: {rule}")
        if fields:
            lines.append(f"  Fields: {fields}")
        if fix:
            lines.append(f"  Fix: {fix}")

    return "\n".join(lines) + "\n"