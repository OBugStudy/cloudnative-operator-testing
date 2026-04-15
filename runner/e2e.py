

import json as _json
import logging
import os

from checkpoint.store import _save_checkpoint
from cluster.env import _setup_cluster_env
from instrumentation.diff import _build_branch_index
from phases.e2e import run_e2e_phase
from runner.common import (
    init_cluster_env,
    load_base_cr,
    load_gsod_context,
    load_operator_config,
    load_or_init_checkpoint,
    setup_runner_workdir,
    teardown_cluster,
)

logger = logging.getLogger(__name__)


def run_e2e(
    config_path: str,
    instrument_info_path: str,
    testplan_checkpoint_path: str,
    context_file: str = "",
    max_rounds: int = 0,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    workdir_base: str = "gsod_output_v5",
    keep_cluster: bool = False,
    reuse_cluster_name: str = "",
    checkpoint_path: str = "",
    debug: bool = False,
    base_cr_path: str = "",
    operator_image: str = "",
    cr_kind: str = "",
    instrument_prefix: str = "",
):
    """End-to-end test runner entry point.

    Args:
        testplan_checkpoint_path: Path to a testplan checkpoint whose
            ``testplan.testcases`` section contains the CR test cases to
            execute.
    """
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("GSOD v5 — 端到端自动化测试")
    logger.info("=" * 70)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "run", operator_name)


    ckpt, ckpt_path = load_or_init_checkpoint(checkpoint_path, workdir)


    if testplan_checkpoint_path and os.path.exists(testplan_checkpoint_path):
        with open(testplan_checkpoint_path, "r", encoding="utf-8") as _f:
            tp_ckpt = _json.load(_f)
        tp_src = tp_ckpt.get("testplan", {})
        if tp_src.get("testcases"):
            ckpt["testplan"] = tp_src
        tc_count = len(ckpt.get("testplan", {}).get("testcases", {}))
        logger.info(f"[e2e] 已合并 testplan checkpoint: {tc_count} 个测试用例")
    else:
        logger.warning(
            "[e2e] 未指定有效的 testplan checkpoint，将尝试从当前 checkpoint 读取"
        )

    config = load_operator_config(config_path)
    branch_meta_index = _build_branch_index(instrument_info_path)

    gsod_context = load_gsod_context(context_file)
    env = init_cluster_env(
        config,
        config_dir,
        gsod_context,
        workdir,
        "gsod-e2e",
        reuse_cluster_name,
        operator_image=operator_image,
    )
    if env is None:
        return False

    cluster_name = env["cluster_name"]
    kubectl_client = env["kubectl_client"]
    namespace = env["namespace"]
    operator_container_name = env["operator_container_name"]
    seed_cr = env["seed_cr"]
    cr_kind = cr_kind or seed_cr.get("kind", "")
    logger.info(f"集群就绪: {cluster_name}, namespace={namespace}, cr_kind={cr_kind}")

    def rebuild_cluster_fn():
        nonlocal env, kubectl_client, namespace, operator_container_name, cluster_name
        logger.warning("[rebuild] 开始拆除并重建集群...")
        teardown_cluster(env, keep_cluster=False, reuse_cluster_name="")
        new_env = _setup_cluster_env(
            config,
            config_dir,
            gsod_context,
            workdir,
            "gsod-e2e",
            operator_image=operator_image,
        )
        if new_env is None:
            return None
        env = new_env
        cluster_name = env["cluster_name"]
        kubectl_client = env["kubectl_client"]
        namespace = env["namespace"]
        operator_container_name = env["operator_container_name"]
        return new_env

    if base_cr_path:
        new_cr = load_base_cr(base_cr_path, seed_cr, namespace, cr_kind, strict=True)
        if new_cr is None:
            return False
        seed_cr = new_cr
        cr_kind = seed_cr.get("kind", cr_kind)

    try:
        run_e2e_phase(
            ckpt=ckpt,
            ckpt_path=ckpt_path,
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            seed_cr=seed_cr,
            cr_kind=cr_kind,
            branch_meta_index=branch_meta_index,
            max_rounds=max_rounds,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
            rebuild_cluster_fn=rebuild_cluster_fn,
        )
    finally:
        _save_checkpoint(ckpt_path, ckpt)
        teardown_cluster(env, keep_cluster, reuse_cluster_name)

    return True