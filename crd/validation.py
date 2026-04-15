import logging
import re
from typing import List

from core.cr_utils import _FIELD_MISSING, _get_current_field_value
from core.patch import _apply_patch_to_cr
from crd.schema import _extract_all_crd_spec_paths, _extract_required_siblings

logger = logging.getLogger(__name__)


def _validate_patch_against_crd(
    patch: dict,
    crd_file: str,
    cr_kind: str,
) -> tuple:
    """Check that every dot-path in patch['set'] exists in the CRD spec schema.

    Returns (clean_patch, error_str) where:
      - clean_patch has invalid set-paths removed
      - error_str is non-empty if any paths were rejected (suitable for error_feedback)

    If crd_file is empty or schema cannot be loaded, returns (patch, "") — no-op.
    """
    if not crd_file:
        return patch, ""
    valid_paths = _extract_all_crd_spec_paths(crd_file, cr_kind)
    if not valid_paths:
        return patch, ""

    bad: List[str] = []
    good: dict = {}
    for dotpath, value in (patch.get("set") or {}).items():

        normalised = re.sub(r"\[\*?\d*\]", "", dotpath)


        parts = normalised.split(".")
        matched = False
        for depth in range(len(parts), 0, -1):
            candidate = ".".join(parts[:depth])
            if candidate in valid_paths:
                matched = True
                break
        if matched:
            good[dotpath] = value
        else:
            bad.append(dotpath)

    if not bad:
        return patch, ""

    clean_patch = {**patch, "set": good}
    error_str = (
        "The following fields you tried to set are NOT defined in the CRD spec and were rejected: "
        + ", ".join(f"`{p}`" for p in bad)
        + ". You MUST only set fields that exist in the CRD. "
        "Do NOT invent Kubernetes sub-object fields (e.g. ephemeralContainers, initContainers sub-fields) "
        "unless they are explicitly listed in the CRD schema."
    )
    logger.warning(f"_validate_patch_against_crd: rejected {len(bad)} path(s): {bad}")
    return clean_patch, error_str


def _repair_required_fields(
    mutated_cr: dict,
    field_base_cr: dict,
    all_required: List[str],
    field_path: str,
) -> tuple:
    """Re-inject any required sibling fields that went missing in mutated_cr.

    Compares mutated_cr against field_base_cr for every required field that
    shares the same ancestor scope as field_path.  If a required field is
    present in field_base_cr but absent (or None) in mutated_cr, its value is
    copied back from field_base_cr.

    Returns (repaired_cr, list_of_repaired_paths).
    """
    import copy as _copy
    import re as _re

    siblings = _extract_required_siblings(all_required, field_path)
    if not siblings:
        return mutated_cr, []

    repaired = _copy.deepcopy(mutated_cr)
    repaired_paths: List[str] = []

    for req_path in siblings:

        norm_req = _re.sub(r"\[\d+\]", "[*]", req_path)
        norm_tgt = _re.sub(r"\[\d+\]", "[*]", field_path)
        if norm_req == norm_tgt:
            continue


        val_in_mutated = _get_current_field_value(repaired, req_path)
        if val_in_mutated is not _FIELD_MISSING and val_in_mutated is not None:
            continue


        val_in_base = _get_current_field_value(field_base_cr, req_path)
        if val_in_base is _FIELD_MISSING or val_in_base is None:
            continue


        try:
            print(f"Repair path: {req_path}")
            repaired = _apply_patch_to_cr(
                repaired, {"set": {req_path: val_in_base}, "delete": []}
            )
            repaired_paths.append(req_path)
        except Exception:
            pass

    return repaired, repaired_paths