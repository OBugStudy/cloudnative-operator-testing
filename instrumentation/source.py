import ctypes
import logging
import os

logger = logging.getLogger(__name__)


def locate_source_code(projectPath, instrumentPath, branchIndex):
    libname = "libinstrument.so"
    lib = ctypes.cdll.LoadLibrary(os.path.abspath(libname))

    instrument_target_func = lib.LocateBranchSource

    instrument_target_func.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    instrument_target_func.restype = ctypes.c_char_p

    result_ptr = instrument_target_func(
        projectPath.encode("utf-8"), instrumentPath.encode("utf-8"), branchIndex
    )
    result_bytes = ctypes.string_at(result_ptr)
    result_str = result_bytes.decode("utf-8")

    return result_str


def _get_branch_source_context(
    project_path: str,
    instrument_dir: str,
    branch_index: int,
) -> str:
    """调用 instrument.locate_source_code 获取分支所在源码上下文。

    Args:
        project_path: operator 源码根目录
        instrument_dir: 插桩目录（与 instrument_info.json 同目录的上级，一般为 /mnt/d/instrument/CassOp 之类）
        branch_index: BranchIndex

    Returns:
        源码上下文字符串，失败时返回空字符串。
    """
    try:
        return locate_source_code(project_path, instrument_dir, branch_index) or ""
    except Exception as e:
        logger.warning(f"locate_source_code({branch_index}) 失败: {e}")
        return ""