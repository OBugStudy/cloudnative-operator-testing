

import logging
import sys
import threading
from collections import deque
from datetime import datetime
from typing import Optional

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.text import Text

    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False


_lock = threading.Lock()
_live: Optional["Live"] = None
_log_buffer: deque = deque(maxlen=28)
_state: dict = {
    "mode": "",
    "operator": "",
    "phase": "",
    "current_op": "",
    "progress_done": 0,
    "progress_total": 0,
    "progress_label": "items",
    "relations": 0,
    "branches_covered": 0,
    "branches_total": 0,
}
_console: Optional["Console"] = None


_LEVEL_STYLE = {
    "DEBUG": "dim",
    "INFO": "bright_white",
    "WARNING": "bold yellow",
    "ERROR": "bold red",
    "CRITICAL": "bold red reverse",
}
_LEVEL_COLOR = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}
_LEVEL_SHORT = {
    "DEBUG": "DBG",
    "INFO": "INF",
    "WARNING": "WRN",
    "ERROR": "ERR",
    "CRITICAL": "CRT",
}


def _build_renderable():
    """将当前状态和日志缓冲构建为 Rich 可渲染对象。"""
    lines = []


    ts = datetime.now().strftime("%H:%M:%S")
    header = Text(justify="left")
    header.append("GSOD v5", style="bold bright_blue")
    if _state["mode"]:
        header.append("  ·  ", style="dim")
        header.append(_state["mode"], style="bold cyan")
    if _state["operator"]:
        header.append("  ·  ", style="dim")
        header.append(_state["operator"], style="bold green")
    header.append(f"   {ts}", style="dim")
    lines.append(header)


    done = _state["progress_done"]
    total = _state["progress_total"]
    label = _state["progress_label"]
    relations = _state["relations"]
    br_cov = _state["branches_covered"]
    br_total = _state["branches_total"]

    stats = Text(justify="left")
    if total > 0:
        pct = done / total
        bar_len = 16
        filled = int(bar_len * pct)
        bar = "█" * filled + "░" * (bar_len - filled)
        stats.append(f"{label} ", style="dim")
        stats.append(f"{done}/{total}", style="bold yellow")
        stats.append(f"  [{bar}]", style="blue")
        stats.append(f"  {int(pct * 100)}%", style="dim")
    if relations > 0:
        stats.append("   关联字段 ", style="dim")
        stats.append(str(relations), style="bold cyan")
    if br_total > 0:
        stats.append("   Branch覆盖 ", style="dim")
        stats.append(f"{br_cov}/{br_total}", style="bold green")

    if len(stats) > 0:
        lines.append(stats)


    current_op = _state.get("current_op", "")
    phase = _state.get("phase", "")
    if current_op or phase:
        op_line = Text(justify="left")
        if phase:
            op_line.append(f"[{phase}]  ", style="bold magenta")
        if current_op:
            op_line.append("⟳  ", style="bold yellow")
            op_line.append(current_op, style="white")
        lines.append(Rule(style="dim"))
        lines.append(op_line)


    lines.append(Rule(style="dim"))
    with _lock:
        buf_snapshot = list(_log_buffer)
    for rec_text in buf_snapshot:
        lines.append(rec_text)

    return Panel(
        Group(*lines),
        border_style="bright_blue",
        padding=(0, 1),
    )


_WAIT_KEYWORDS = (
    "等待",
    "⏱",
    "waiting",
    "wait",
    "稳定等待",
    "就绪等待",
    "删除等待",
    "收集等待",
)


def _is_wait_message(msg: str) -> bool:
    """Return True if *msg* describes a waiting / timing step."""
    lower = msg.lower()
    return any(kw in msg or kw in lower for kw in _WAIT_KEYWORDS)


def _format_log_record(record: logging.LogRecord) -> "Text":
    """将 LogRecord 格式化为带颜色的 Rich Text 对象。"""
    ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
    level = record.levelname
    short = _LEVEL_SHORT.get(level, level[:3])
    msg = record.getMessage()

    t = Text(justify="left", no_wrap=True, overflow="fold")
    t.append(f"{ts} ", style="dim")
    t.append(f"{short} ", style=_LEVEL_COLOR.get(level, "white"))
    if level == "INFO" and _is_wait_message(msg):
        t.append(msg, style="bold bright_cyan")
    else:
        t.append(msg, style=_LEVEL_STYLE.get(level, "white"))
    return t


class _RichLiveHandler(logging.Handler):
    """将日志记录路由进 Live 动态面板的 logging.Handler。"""

    def emit(self, record: logging.LogRecord):
        try:
            text = _format_log_record(record)
            with _lock:
                _log_buffer.append(text)
            if _live is not None:
                _live.update(_build_renderable())
        except Exception:
            self.handleError(record)


class _FallbackHandler(logging.StreamHandler):
    """Rich 不可用时的回退 Handler（普通彩色输出）。"""

    RESET = "\033[0m"
    COLORS = {
        "DEBUG": "\033[90m",
        "INFO": "\033[37m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[1;31m",
    }

    def emit(self, record: logging.LogRecord):
        try:
            ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
            level = record.levelname
            color = self.COLORS.get(level, "")
            msg = record.getMessage()
            self.stream.write(f"{color}{ts} [{level}] {msg}{self.RESET}\n")
            self.stream.flush()
        except Exception:
            self.handleError(record)


def setup_rich_logging(
    mode: str = "",
    operator: str = "",
    log_level: int = logging.INFO,
) -> None:
    """初始化 Rich Live 日志系统，替换 root logger 的 handlers。

    Args:
        mode: 运行模式字符串，如 "explore-all"、"run"。
        operator: Operator 名称，用于面板标题显示。
        log_level: 日志级别（默认 INFO）。
    """
    global _live, _console, _state

    _state["mode"] = mode
    _state["operator"] = operator

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    root_logger.handlers.clear()

    _is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
    if not _RICH_AVAILABLE or not _is_tty:
        handler = _FallbackHandler(stream=sys.stderr)
        handler.setLevel(log_level)
        root_logger.addHandler(handler)
        return

    _console = Console(stderr=True, highlight=False)
    live_handler = _RichLiveHandler()
    live_handler.setLevel(log_level)
    root_logger.addHandler(live_handler)

    global _live
    _live = Live(
        _build_renderable(),
        console=_console,
        refresh_per_second=4,
        transient=False,
        vertical_overflow="crop",
    )
    _live.start()


def stop_rich_logging() -> None:
    """停止 Live 显示（程序退出前调用）。"""
    global _live
    if _live is not None:
        try:
            _live.stop()
        except Exception:
            pass
        _live = None


def update_status(
    phase: Optional[str] = None,
    current_op: Optional[str] = None,
    mode: Optional[str] = None,
    operator: Optional[str] = None,
) -> None:
    """更新面板顶部的状态信息（不影响日志缓冲）。"""
    changed = False
    if phase is not None and _state["phase"] != phase:
        _state["phase"] = phase
        changed = True
    if current_op is not None and _state["current_op"] != current_op:
        _state["current_op"] = current_op
        changed = True
    if mode is not None and _state["mode"] != mode:
        _state["mode"] = mode
        changed = True
    if operator is not None and _state["operator"] != operator:
        _state["operator"] = operator
        changed = True
    if changed and _live is not None:
        _live.update(_build_renderable())


def update_progress(
    done: int,
    total: int,
    label: str = "items",
    relations: Optional[int] = None,
    branches_covered: Optional[int] = None,
    branches_total: Optional[int] = None,
) -> None:
    """更新进度条和统计数字。"""
    _state["progress_done"] = done
    _state["progress_total"] = total
    _state["progress_label"] = label
    if relations is not None:
        _state["relations"] = relations
    if branches_covered is not None:
        _state["branches_covered"] = branches_covered
    if branches_total is not None:
        _state["branches_total"] = branches_total
    if _live is not None:
        _live.update(_build_renderable())