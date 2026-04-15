from contextlib import contextmanager
import time
import logging

logger = logging.getLogger(__name__)


def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as a human-readable string."""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


@contextmanager
def _timed_step(label: str, extra: str = ""):
    """Context manager that logs the wall-clock time for a labelled step.

    Usage::
        with _timed_step("LLM call"):
            result = _call_llm_for_branch_flip(prompt)
    """
    _t0 = time.monotonic()
    suffix = f" ({extra})" if extra else ""
    logger.info(f"⏱  [{label}{suffix}] 开始...")
    try:
        yield
    finally:
        elapsed = time.monotonic() - _t0
        logger.info(f"⏱  [{label}{suffix}] 完成  {_fmt_elapsed(elapsed)}")