import json
import logging
import os
import signal
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


def _default_checkpoint() -> dict:
    """返回初始 checkpoint 数据结构。"""
    return {
        "version": 5,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),

        "phase1": {
            "completed_fields": [],
            "mutation_log": [],
        },

        "phase2": {
            "baseline_instr": None,
            "coverage_map": {},
            "test_plan": [],
            "explore_log": [],
            "completed_combos": [],
        },
        "testplan": {
            "coverage_map": {},
            "testcases": {},
            "targets": {},
            "stuck_count": 0,
            "next_id": 1,
            "baseline_collected": False,
            "branch_coverage_history": [],
            "target_coverage_history": [],
            "round_n": 0,

            "llm_stats": {
                "cr_gen_attempts": 0,
                "cr_gen_produced": 0,
                "cr_apply_success": 0,
            },

            "target_hit_stats": {
                "attempts": 0,
                "hits": 0,
            },
        },

        "field_relations": {},
        "current_cr_yaml": "",
    }


def _load_checkpoint(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            ckpt = json.load(f)
        logger.info(f"[checkpoint] 已加载: {path}")
        return ckpt
    return _default_checkpoint()


def _save_checkpoint(path: str, ckpt: dict):
    ckpt["updated_at"] = datetime.now().isoformat()
    _safe_write_json(path, ckpt)


def _safe_write_json(path: str, data: dict) -> None:
    """Write JSON to *path* in a way that defers KeyboardInterrupt until the
    write completes, so Ctrl+C cannot leave the checkpoint half-written.

    On non-main threads (where signal handlers cannot be set), falls back to
    writing without signal protection — the worst case is a truncated file on
    that thread, but checkpoints are only written from the main thread in normal
    operation.
    """
    _kbi_pending = []

    def _defer_kbi(signum, frame):
        _kbi_pending.append(True)

    _old_handler = None
    _on_main_thread = threading.current_thread() is threading.main_thread()
    if _on_main_thread:
        try:
            _old_handler = signal.signal(signal.SIGINT, _defer_kbi)
        except (OSError, ValueError):
            _on_main_thread = False

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
    finally:
        if _on_main_thread and _old_handler is not None:
            try:
                signal.signal(signal.SIGINT, _old_handler)
            except (OSError, ValueError):
                pass
        if _kbi_pending:
            raise KeyboardInterrupt


def _branch_baseline_crs_path(config_path: str) -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(config_path)), "branch_baseline_crs.json"
    )


def _load_branch_baseline_crs(config_path: str) -> dict:
    path = _branch_baseline_crs_path(config_path)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as _f:
                return json.load(_f) or {}
        except Exception:
            return {}
    return {}


def _save_branch_baseline_crs(config_path: str, data: dict) -> None:
    _save_json(_branch_baseline_crs_path(config_path), data)


def _update_branch_baseline_crs(config_path: str, instr: dict, cr_yaml: str) -> dict:
    """Update branch_baseline_crs: for every branch covered in instr, if no entry
    exists yet, record cr_yaml as the baseline CR for that branch.

    Returns the (possibly updated) mapping.
    """
    if not config_path:
        return {}
    data = _load_branch_baseline_crs(config_path)
    updated = False
    for t in instr.get("traces", []):
        bi = str(t.get("branch_index", ""))
        if bi and bi not in data:
            data[bi] = cr_yaml
            updated = True
    if updated:
        _save_branch_baseline_crs(config_path, data)
        logger.info(
            f"branch_baseline_crs 已更新: {_branch_baseline_crs_path(config_path)}"
        )
    return data


def _save_json(path: str, data: dict):
    """安全保存 JSON（中断安全）"""
    try:
        _safe_write_json(path, data)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logger.error(f"保存 JSON 失败 ({path}): {e}")