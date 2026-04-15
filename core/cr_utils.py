from typing import List

import yaml


def _flatten_cr_spec(cr: dict) -> dict:
    """展开 CR spec 字段为 {field_path: value}，路径格式与 CRD fields 一致。
    数组使用 [*] 符号，值为整个 list。同时保留每个中间节点路径。
    """
    out = {}

    def _walk(obj, prefix: str):
        out[prefix] = obj
        if isinstance(obj, dict):
            for k, v in obj.items():
                _walk(v, f"{prefix}.{k}")
        elif isinstance(obj, list):
            arr_prefix = f"{prefix}[*]"
            out[arr_prefix] = obj
            for item in obj:
                if isinstance(item, dict):
                    for k, v in item.items():
                        _walk(v, f"{arr_prefix}.{k}")

    spec = cr.get("spec", {})
    if isinstance(spec, dict):
        for k, v in spec.items():
            _walk(v, f"spec.{k}")
    return out


def _cr_changed_fields(cr_before: dict, cr_after: dict) -> List[str]:
    """Return the leaf-only list of spec field paths that differ between two CRs.

    Ancestor paths are filtered out so only the most specific changed paths remain.
    """
    before = _flatten_cr_spec(cr_before)
    after = _flatten_cr_spec(cr_after)
    all_changed = [
        fp
        for fp in set(list(before.keys()) + list(after.keys()))
        if before.get(fp) != after.get(fp)
    ]
    changed_set = set(all_changed)
    return sorted(
        fp
        for fp in all_changed
        if not any(other != fp and other.startswith(fp) for other in changed_set)
    )


def _collapse_free_form_sub_paths(
    paths: List[str], free_form_map_paths: set
) -> List[str]:
    """Collapse sub-keys of free-form map fields to the map field itself.

    E.g. with free_form_map_paths={'spec.additionalAnnotations'}:
      ['spec.additionalAnnotations.example.com/x', 'spec.additionalAnnotations.team']
      → ['spec.additionalAnnotations']

    Non-free-form paths are returned unchanged.  Deduplicates the result.
    """
    if not free_form_map_paths:
        return paths
    seen: set = set()
    result: List[str] = []
    for fp in paths:
        effective = fp
        for ffm in free_form_map_paths:
            if fp.startswith(ffm + ".") or fp.startswith(ffm + "["):
                effective = ffm
                break
        if effective not in seen:
            seen.add(effective)
            result.append(effective)
    return result


_FIELD_MISSING = object()


def _get_current_field_value(base_cr: dict, field_path: str):
    """Extract the current value of `field_path` from base_cr, or _FIELD_MISSING.

    Handles:
      - plain dot paths:  spec.size
      - [*] wildcard:     spec.foo[*].bar  → value from first list element
      - [N] index:        spec.foo[0].bar  → value from element N
    Returns the value, or _FIELD_MISSING if the path doesn't exist.
    """
    node = base_cr
    for seg in field_path.split("."):
        if node is _FIELD_MISSING:
            break
        if "[" in seg:
            key, rest = seg.split("[", 1)
            idx_str = rest.rstrip("]")
            try:
                idx = int(idx_str) if idx_str not in ("", "*") else 0
            except ValueError:
                idx = 0
            if not isinstance(node, dict) or key not in node:
                node = _FIELD_MISSING
                break
            lst = node[key]
            if not isinstance(lst, list) or idx >= len(lst):
                node = _FIELD_MISSING
                break
            node = lst[idx]
        else:
            if not isinstance(node, dict) or seg not in node:
                node = _FIELD_MISSING
                break
            node = node[seg]
    return node


def _fmt_current_value(base_cr: dict, field_path: str) -> str:
    """Return a human-readable string for the current value of field_path,
    or empty string if the field is absent."""
    val = _get_current_field_value(base_cr, field_path)
    if val is _FIELD_MISSING:
        return ""
    try:
        raw = yaml.dump(val, default_flow_style=True).strip()
    except Exception:
        raw = repr(val)
    return raw


def _field_exists_in_cr(cr: dict, field_path: str) -> bool:
    """Return True if field_path resolves to a non-None value inside cr.

    Supports paths like spec.foo, spec.foo.bar, spec.foo[*].bar
    (array segments use the first element for existence check).
    """
    parts = field_path.split(".")
    obj = cr
    for part in parts:
        if obj is None:
            return False
        if part.endswith("[*]"):
            key = part[:-3]
            if not isinstance(obj, dict) or key not in obj:
                return False
            obj = obj[key]
            if not isinstance(obj, list) or not obj:
                return False
            obj = obj[0]
        else:
            if not isinstance(obj, dict) or part not in obj:
                return False
            obj = obj[part]
    return obj is not None