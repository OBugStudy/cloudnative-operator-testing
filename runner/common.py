

import json
import logging
import os
from datetime import datetime

import yaml

from acto.lib.operator_config import OperatorConfig
from checkpoint.store import (
    _default_checkpoint,
    _load_checkpoint,
    _save_checkpoint,
)
from cluster.env import _attach_cluster_env, _setup_cluster_env
from instrumentation.diff import _build_branch_index
from instrumentation.loader import load_instrument_info

logger = logging.getLogger(__name__)


def setup_runner_workdir(workdir_base: str, prefix: str, operator_name: str) -> str:
    """创建带时间戳的工作目录，返回目录绝对路径。"""
    os.makedirs(workdir_base, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    workdir = os.path.join(workdir_base, f"{prefix}-{operator_name}-{ts_str}")
    os.makedirs(workdir, exist_ok=True)
    logger.info(f"工作目录: {workdir}")
    return workdir


def load_or_init_checkpoint(checkpoint_path: str, workdir: str) -> tuple:
    """加载已有 checkpoint 或创建新 checkpoint。

    返回 (ckpt, resolved_ckpt_path)。
    """
    resolved = checkpoint_path or os.path.join(workdir, "checkpoint.json")
    if checkpoint_path and os.path.exists(checkpoint_path):
        ckpt = _load_checkpoint(checkpoint_path)
        resolved = checkpoint_path
    elif os.path.exists(resolved):
        ckpt = _load_checkpoint(resolved)
    else:
        ckpt = _default_checkpoint()
        _save_checkpoint(resolved, ckpt)
    logger.info(f"Checkpoint: {resolved}")
    return ckpt, resolved


def load_operator_config(config_path: str) -> OperatorConfig:
    """从 JSON 文件加载并验证 OperatorConfig。"""
    with open(config_path, "r", encoding="utf-8") as f:
        return OperatorConfig.model_validate(json.load(f))


def load_instrumentation(instrument_info_path: str) -> dict:
    """加载插桩元数据并构建 branch index，返回 branch_meta_index。"""
    load_instrument_info(instrument_info_path)
    branch_meta_index = _build_branch_index(instrument_info_path)
    logger.info(f"插桩 branch 总数: {len(branch_meta_index)}")
    return branch_meta_index


def load_gsod_context(context_file: str) -> dict:
    """加载 context.json，返回 gsod_context dict。

    若文件不存在则返回空骨架。
    """
    if context_file and os.path.exists(context_file):
        with open(context_file, "r", encoding="utf-8") as f:
            ctx = json.load(f)
        ctx["preload_images"] = set(ctx.get("preload_images", []))
        return ctx
    return {"preload_images": set()}


def init_cluster_env(
    config: OperatorConfig,
    config_dir: str,
    gsod_context: dict,
    workdir: str,
    cluster_prefix: str,
    reuse_cluster_name: str,
    operator_image: str = "",
) -> dict | None:
    """初始化集群环境（复用或新建），返回 env dict 或 None。"""
    if reuse_cluster_name:
        logger.info(f"[初始化] 尝试复用集群: {reuse_cluster_name}")
        env = _attach_cluster_env(config, config_dir, reuse_cluster_name, workdir)
        if env is None:
            logger.warning(f"[初始化] 集群 {reuse_cluster_name} 不存在，改为新建...")
            env = _setup_cluster_env(
                config,
                config_dir,
                gsod_context,
                workdir,
                reuse_cluster_name,
                operator_image=operator_image,
            )
    else:
        logger.info("[初始化] 创建集群...")
        env = _setup_cluster_env(
            config,
            config_dir,
            gsod_context,
            workdir,
            cluster_prefix,
            operator_image=operator_image,
        )
    if env is None:
        logger.error("集群初始化失败")
    return env


def load_base_cr(
    base_cr_path: str,
    seed_cr: dict,
    namespace: str,
    cr_kind: str,
    strict: bool = False,
) -> dict | None:
    """从文件加载完全体 base CR，合并 metadata，返回新 seed_cr。

    若文件不存在或无效：strict=True 时返回 None，否则返回原 seed_cr。
    """
    if not base_cr_path:
        return seed_cr
    if not os.path.exists(base_cr_path):
        logger.error(f"--base-cr 文件不存在: {base_cr_path}")
        return None if strict else seed_cr
    with open(base_cr_path, "r", encoding="utf-8") as _f:
        _base = yaml.safe_load(_f)
    if not isinstance(_base, dict) or "spec" not in _base:
        logger.error(
            f"--base-cr 文件无效（应为含有 spec 的 YAML dict）: {base_cr_path}"
        )
        return None if strict else seed_cr
    _base.setdefault("metadata", {})["name"] = seed_cr.get("metadata", {}).get(
        "name", "test-cluster"
    )
    _base["metadata"].setdefault("namespace", namespace)
    if not _base.get("kind"):
        _base["kind"] = cr_kind
    logger.info(f"[base_cr] 使用完全体 base CR: {base_cr_path}")
    spec_count = (
        len(_base.get("spec", {}).keys())
        if isinstance(_base.get("spec"), dict)
        else "?"
    )
    logger.info(f"[base_cr] spec 字段数: {spec_count}")
    return _base


def teardown_cluster(env: dict, keep_cluster: bool, reuse_cluster_name: str) -> None:
    """集群清理：根据参数决定保留或删除集群。"""
    cluster_name = env.get("cluster_name", "?")
    if keep_cluster or reuse_cluster_name:
        logger.info(f"集群 {cluster_name} 保留")
        return
    try:
        cluster = env.get("cluster")
        if cluster:
            cluster.delete_cluster(cluster_name, env.get("kubeconfig", ""))
        logger.info(f"集群 {cluster_name} 已删除")
    except Exception as e:
        logger.warning(f"删除集群失败: {e}")