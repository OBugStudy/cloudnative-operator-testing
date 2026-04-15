import json
import logging
import os
from typing import List, Optional

import yaml

logger = logging.getLogger(__name__)


def _load_crd_schema_root(crd_file: str, cr_kind: str) -> dict:
    """Load and return the openAPIV3Schema root for cr_kind from crd_file. Returns {} on failure."""
    if not crd_file or not os.path.exists(crd_file):
        return {}
    try:
        with open(crd_file, "r", encoding="utf-8") as f:
            raw = f.read()
        docs = list(yaml.safe_load_all(raw))
    except Exception:
        return {}
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "CustomResourceDefinition":
            continue
        if (
            doc.get("spec", {}).get("names", {}).get("kind", "").lower()
            != cr_kind.lower()
        ):
            continue
        for ver in doc.get("spec", {}).get("versions", []):
            s = ver.get("schema", {}).get("openAPIV3Schema", {})
            if s:
                return s
    return {}


def _extract_all_crd_spec_paths(crd_file: str, cr_kind: str, max_depth: int = 6) -> set:
    """Return a set of all valid dot-paths under spec defined in the CRD schema.

    Paths are normalised: array items are represented without indices
    (e.g. spec.containers rather than spec.containers[0]).
    Returns empty set if CRD cannot be loaded.
    """
    schema_root = _load_crd_schema_root(crd_file, cr_kind)
    if not schema_root:
        return set()

    spec_schema = schema_root.get("properties", {}).get("spec", {})
    paths: set = set()

    def _walk(node: dict, prefix: str, depth: int) -> None:
        if not isinstance(node, dict) or depth > max_depth:
            return
        paths.add(prefix)
        for fname, fschema in (node.get("properties") or {}).items():
            child_path = f"{prefix}.{fname}"
            _walk(fschema, child_path, depth + 1)
        items = node.get("items")
        if isinstance(items, dict):
            _walk(items, prefix, depth)

    _walk(spec_schema, "spec", 1)
    return paths


def _extract_free_form_map_paths(
    crd_file: str, cr_kind: str, max_depth: int = 6
) -> set:
    """Return the set of paths whose schema node is a free-form map.

    A node is a free-form map when it has `additionalProperties` defined but
    NO explicit `properties` children.  Examples: additionalAnnotations,
    additionalLabels, nodeAffinity freeform maps.

    Sub-paths of these nodes (e.g. spec.additionalAnnotations.mycompany.com/x)
    are arbitrary user-defined keys, not declared CRD schema fields.  Callers
    should treat any path that starts with one of these as a sub-key of the map,
    not as an independent schema field.
    """
    schema_root = _load_crd_schema_root(crd_file, cr_kind)
    if not schema_root:
        return set()

    spec_schema = schema_root.get("properties", {}).get("spec", {})
    free_form: set = set()

    def _walk(node: dict, prefix: str, depth: int) -> None:
        if not isinstance(node, dict) or depth > max_depth:
            return
        has_props = bool(node.get("properties"))
        has_additional = "additionalProperties" in node
        if has_additional and not has_props:
            free_form.add(prefix)
            return
        for fname, fschema in (node.get("properties") or {}).items():
            _walk(fschema, f"{prefix}.{fname}", depth + 1)
        items = node.get("items")
        if isinstance(items, dict):
            _walk(items, prefix, depth)

    _walk(spec_schema, "spec", 1)
    return free_form


def _extract_crd_required_fields(
    crd_file: str, cr_kind: str, max_depth: int = 0
) -> List[str]:
    """Walk CRD openAPIV3Schema and return required field paths under spec.

    A path is required if it appears in the `required` list of its parent object.
    Array item required fields are emitted with [*] notation, e.g.
    "spec.containers[*].name".
    max_depth=0 means unlimited depth (default).

    Returns a sorted list of dot-separated paths, e.g. ["spec.clusterName", "spec.size"].
    """
    if not crd_file or not os.path.exists(crd_file):
        return []
    try:
        with open(crd_file, "r", encoding="utf-8") as f:
            raw = f.read()
        docs = list(yaml.safe_load_all(raw))
    except Exception:
        return []

    crd_doc = None
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "CustomResourceDefinition":
            continue
        names = doc.get("spec", {}).get("names", {})
        if names.get("kind", "").lower() == cr_kind.lower():
            crd_doc = doc
            break
    if crd_doc is None:
        return []

    versions = crd_doc.get("spec", {}).get("versions", [])
    schema_root = {}
    for ver in versions:
        s = ver.get("schema", {}).get("openAPIV3Schema", {})
        if s:
            schema_root = s
            break
    if not schema_root:
        return []

    spec_schema = schema_root.get("properties", {}).get("spec", {})

    required_paths: List[str] = []
    _seen: set = set()

    def _walk(node: dict, prefix: str, depth: int) -> None:
        if not isinstance(node, dict):
            return
        if max_depth > 0 and depth > max_depth:
            return

        node_id = id(node)
        if node_id in _seen:
            return
        _seen.add(node_id)
        required_here = node.get("required") or []
        props = node.get("properties") or {}
        for field in required_here:
            path = f"{prefix}.{field}"
            required_paths.append(path)
        for fname, fschema in props.items():
            path = f"{prefix}.{fname}"
            if isinstance(fschema, dict):
                _walk(fschema, path, depth + 1)

                items = fschema.get("items")
                if isinstance(items, dict):
                    _walk(items, f"{path}[*]", depth + 1)
        _seen.discard(node_id)

    _walk(spec_schema, "spec", 1)
    return sorted(set(required_paths))


def _extract_required_siblings(all_required: List[str], field_path: str) -> List[str]:
    """Return the subset of required fields that share the nearest scoped ancestor
    with field_path.  This strips away the 342-field firehose and keeps only the
    fields the LLM *actually* needs to know about when mutating field_path.

    Algorithm:
      1. Normalise paths by replacing [*] and [N] with [*].
      2. Walk up the target path removing one segment at a time until we find a
         prefix that has at least one required-field match.
      3. Return all required fields whose normalised path starts with that prefix.

    Example: target = "spec.foo[*].bar[*].val"
      tries "spec.foo[*].bar[*]" → returns required fields under that scope.
      If none, tries "spec.foo[*]" → etc.
    """
    import re as _re

    def _norm(p: str) -> str:
        return _re.sub(r"\[\d+\]", "[*]", p)

    target_norm = _norm(field_path)

    norm_required = [_norm(r) for r in all_required]


    segments = target_norm.replace("[*]", "[*]\x00").split(".")


    segments = [s.rstrip("\x00") for s in segments]


    for end in range(len(segments) - 1, 0, -1):
        prefix = ".".join(segments[:end])

        matches = [
            orig
            for orig, norm in zip(all_required, norm_required)
            if norm.startswith(prefix + ".") or norm.startswith(prefix + "[")
        ]
        if matches:
            return sorted(set(matches))

    return all_required


def _is_field_optional_in_crd(crd_file: str, cr_kind: str, field_path: str) -> bool:
    """Return True if field_path is NOT in the 'required' list of its parent in the CRD schema.

    Returns True (assume optional) when CRD is unavailable or schema cannot be navigated.
    """
    if not crd_file or not os.path.exists(crd_file):
        return True
    try:
        with open(crd_file, "r", encoding="utf-8") as _f:
            docs = list(yaml.safe_load_all(_f.read()))
    except Exception:
        return True

    crd_doc = None
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "CustomResourceDefinition":
            continue
        if (
            doc.get("spec", {}).get("names", {}).get("kind", "").lower()
            == cr_kind.lower()
        ):
            crd_doc = doc
            break
    if crd_doc is None:
        return True

    versions = crd_doc.get("spec", {}).get("versions", [])
    schema_root: dict = {}
    for ver in versions:
        s = ver.get("schema", {}).get("openAPIV3Schema", {})
        if s:
            schema_root = s
            break
    if not schema_root:
        return True


    parts = field_path.split(".")
    node = schema_root
    for part in parts[:-1]:
        if not node:
            return True
        if part == "spec":
            node = node.get("properties", {}).get("spec", {})
        elif part.endswith("[*]"):
            key = part[:-3]
            node = node.get("properties", {}).get(key, {}).get("items", {})
        else:
            node = node.get("properties", {}).get(part, {})

    leaf = parts[-1]
    if leaf.endswith("[*]"):
        leaf = leaf[:-3]
    return leaf not in node.get("required", [])


def _extract_crd_schema_for_fields(
    crd_file: str,
    cr_kind: str,
    field_paths: List[str],
) -> str:
    """从 CRD 文件中提取与 field_paths 相关的 schema 片段，以 YAML 返回。

    Args:
        crd_file:    CRD 文件路径（可以是多文档 YAML）
        cr_kind:     CR 的 kind（例如 CassandraDatacenter），用于定位正确的 CRD
        field_paths: 字段路径列表（格式 spec.xxx 或 spec.xxx.yyy）

    Returns:
        包含相关字段 schema 定义的 YAML 字符串；失败时返回空字符串。
    """
    if not crd_file or not os.path.exists(crd_file):
        return ""
    try:
        with open(crd_file, "r", encoding="utf-8") as f:
            raw = f.read()
        docs = list(yaml.safe_load_all(raw))
    except Exception as e:
        logger.warning(f"_extract_crd_schema_for_fields: 读取 CRD 失败: {e}")
        return ""


    crd_doc = None
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "CustomResourceDefinition":
            continue
        names = doc.get("spec", {}).get("names", {})
        if names.get("kind", "").lower() == cr_kind.lower():
            crd_doc = doc
            break

    if crd_doc is None:
        logger.warning(f"_extract_crd_schema_for_fields: 未找到 kind={cr_kind} 的 CRD")
        return ""


    versions = crd_doc.get("spec", {}).get("versions", [])
    schema_root = {}
    for ver in versions:
        s = ver.get("schema", {}).get("openAPIV3Schema", {})
        if s:
            schema_root = s
            break

    if not schema_root:
        return ""

    def _walk(node: dict, parts: List[str]) -> Optional[dict]:
        """沿 parts 路径（不含 spec 前缀）递归下钻 schema，返回目标节点。"""
        if not parts or not isinstance(node, dict):
            return node
        head, *rest = parts
        props = node.get("properties", {})
        if head in props:
            return _walk(props[head], rest)

        items = node.get("items", {})
        if isinstance(items, dict) and head in items.get("properties", {}):
            return _walk(items["properties"][head], rest)
        return None

    spec_schema = schema_root.get("properties", {}).get("spec", {})

    extracted: dict = {}
    for fp in field_paths:

        parts = fp.split(".")
        if parts and parts[0] == "spec":
            parts = parts[1:]
        if not parts:
            continue
        node = _walk(spec_schema, parts)
        if node is not None:
            ap = node.get("additionalProperties")
            ap_summary: Optional[str] = None
            if isinstance(ap, dict):
                ap_type = ap.get("type", "string")
                ap_summary = f"additionalProperties: {{type: {ap_type}}}"
            elif ap is True:
                ap_summary = "additionalProperties: true"
            extracted[fp] = {
                "type": node.get("type", "object"),
                "description": node.get("description", "")[:200],
                "properties": list(node.get("properties", {}).keys()) or None,
                "additionalProperties": ap_summary,
                "enum": node.get("enum"),
                "format": node.get("format"),
                "minimum": node.get("minimum"),
                "maximum": node.get("maximum"),
                "items": (
                    node.get("items", {}).get("type")
                    if isinstance(node.get("items"), dict)
                    else None
                ),
            }

            extracted[fp] = {k: v for k, v in extracted[fp].items() if v is not None}

    if not extracted:
        return ""
    return yaml.dump(extracted, allow_unicode=True, sort_keys=False)


def extract_crd_spec_fields(context_path: str) -> List[dict]:
    """从 context.json 提取 CRD spec 下所有字段

    Returns:
        list of dict, 每项包含:
            path: 字段路径 (如 spec.size)
            type: CRD 定义的类型
            description: 描述
    """
    with open(context_path, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    crd_body = ctx.get("crd", {}).get("body", {})
    spec = crd_body.get("spec", {})
    versions = spec.get("versions", [])
    if not versions:
        logger.warning("CRD 中无 versions 信息")
        return []

    schema = versions[0].get("schema", {}).get("openAPIV3Schema", {})
    spec_schema = schema.get("properties", {}).get("spec", {})
    if not spec_schema:
        logger.warning("CRD 中无 spec schema")
        return []

    fields = []

    def traverse(schema_node, prefix, depth):
        if depth > 8:
            return
        props = schema_node.get("properties", {})
        for name, prop in props.items():
            fp = f"{prefix}.{name}"
            ft = prop.get("type", "object")
            fields.append(
                {
                    "path": fp,
                    "type": ft,
                    "description": prop.get("description", "")[:120],
                    "depth": depth,
                }
            )
            if ft == "object" and "properties" in prop:
                traverse(prop, fp, depth + 1)
            elif ft == "array" and "items" in prop:
                items = prop["items"]
                if items.get("type") == "object" and "properties" in items:
                    traverse(items, f"{fp}[*]", depth + 1)

    traverse(spec_schema, "spec", 1)
    return fields


def get_crd_file_path(config, config_dir):
    """从 operator 配置中定位 CRD 文件"""
    if hasattr(config.deploy, "steps"):
        for step in config.deploy.steps:
            if "apply" in step and step["apply"].get("operator"):
                return step["apply"].get("file")
    for fn in ["operator.yaml", "bundle.yaml", "crd.yaml"]:
        p = os.path.join(config_dir, fn)
        if os.path.exists(p):
            return p
    return None