import json as _json
import logging
import os

from checkpoint.store import _save_checkpoint
from cluster.env import _setup_cluster_env
from crd.schema import extract_crd_spec_fields
from instrumentation.collector import port_forward_fault_manager
from instrumentation.diff import _build_branch_index
from phases.fault import run_fault_phase
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


def run_fault(
    config_path: str,
    instrument_info_path: str,
    testplan_checkpoint_path: str,
    fault_types: list,
    fault_manager_url: str = "",
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
    db_dir: str = "",
):
    """Fault-injection test runner: loads testplan testcases, injects faults each round."""
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("GSOD v5 — Fault-Injection Testing")
    logger.info(f"  fault_types : {fault_types}")
    logger.info(f"  testplan ckpt: {testplan_checkpoint_path}")
    logger.info("=" * 70)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "ft", operator_name)


    ckpt, ckpt_path = load_or_init_checkpoint(checkpoint_path, workdir)


    if testplan_checkpoint_path and os.path.exists(testplan_checkpoint_path):
        with open(testplan_checkpoint_path, "r", encoding="utf-8") as _f:
            tp_ckpt = _json.load(_f)
        tp_src = tp_ckpt.get("testplan", {})
        if tp_src.get("testcases"):
            ckpt["testplan"] = tp_src
        logger.info(
            f"[fault] 已合并 testplan checkpoint: "
            f"{len(ckpt.get('testplan', {}).get('testcases', {}))} 个测试用例"
        )
    else:
        logger.warning(
            "[fault] 未指定有效的 testplan checkpoint，将尝试从当前 checkpoint 读取"
        )


    if not ckpt.get("field_relations"):
        ckpt["field_relations"] = {}
    field_relations: dict = ckpt["field_relations"]

    if not field_relations:
        _tp_fr = (
            tp_ckpt
            if testplan_checkpoint_path and os.path.exists(testplan_checkpoint_path)
            else {}
        ).get("field_relations", {})
        if _tp_fr:
            ckpt["field_relations"].update(_tp_fr)
            logger.info(
                f"[fault] field_relations 从 testplan checkpoint 加载: {len(field_relations)} 条"
            )

    declared_field_paths: set = set()
    if context_file and os.path.exists(context_file):
        _crd_fields = extract_crd_spec_fields(context_file)
        declared_field_paths = {f["path"] for f in _crd_fields}

    config = load_operator_config(config_path)
    branch_meta_index = _build_branch_index(instrument_info_path)

    gsod_context = load_gsod_context(context_file)
    env = init_cluster_env(
        config,
        config_dir,
        gsod_context,
        workdir,
        "gsod-ft",
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
            "gsod-ft",
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


    _needs_fault_mgr = any(ft in fault_types for ft in ("reconnect", "delay"))
    _auto_fwd = _needs_fault_mgr and not fault_manager_url


    _db_dir = db_dir
    if not _db_dir and config_path:
        _auto_db = os.path.join(
            os.path.dirname(os.path.abspath(config_path)), "testcase_db"
        )
        _db_dir = _auto_db
        logger.info(f"[testcase_db] 自动使用 DB 目录: {_db_dir}")

    def _run(resolved_fm_url: str) -> None:
        try:
            run_fault_phase(
                ckpt=ckpt,
                ckpt_path=ckpt_path,
                kubectl_client=kubectl_client,
                namespace=namespace,
                cluster_name=cluster_name,
                operator_container_name=operator_container_name,
                seed_cr=seed_cr,
                cr_kind=cr_kind,
                branch_meta_index=branch_meta_index,
                fault_types=fault_types,
                fault_manager_url=resolved_fm_url,
                max_rounds=max_rounds,
                wait_sec=wait_sec,
                collect_max_wait=collect_max_wait,
                instrument_prefix=instrument_prefix,
                rebuild_cluster_fn=rebuild_cluster_fn,
                field_relations=field_relations or None,
                declared_field_paths=declared_field_paths or None,
                db_dir=_db_dir,
            )
        finally:
            _save_checkpoint(ckpt_path, ckpt)
            teardown_cluster(env, keep_cluster, reuse_cluster_name)

    if _auto_fwd:
        logger.info(
            "[fault] fault-manager-url 未指定，自动转发集群内 fault-manager 服务..."
        )
        try:
            with port_forward_fault_manager(cluster_name=cluster_name) as _fm_port:
                resolved_url = f"http://localhost:{_fm_port}"
                logger.info("[fault] fault-manager 转发就绪: %s", resolved_url)
                _run(resolved_url)
        except (RuntimeError, TimeoutError) as _fwd_err:
            logger.warning(
                "[fault] fault-manager 端口转发失败: %s — reconnect/delay 任务将跳过",
                _fwd_err,
            )
            _run("")
    else:
        _run(fault_manager_url)

    return True