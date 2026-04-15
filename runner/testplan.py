import json as _json
import logging
import os

from checkpoint.store import _save_checkpoint
from cluster.env import _setup_cluster_env
from crd.schema import extract_crd_spec_fields, get_crd_file_path
from llm.constraints import ensure_constraints
from phases.testplan import run_testplan_phase
from report.testplan import generate_testplan_report
from runner.common import (
    init_cluster_env,
    load_base_cr,
    load_gsod_context,
    load_instrumentation,
    load_operator_config,
    load_or_init_checkpoint,
    setup_runner_workdir,
    teardown_cluster,
)

logger = logging.getLogger(__name__)


def run_testplan(
    config_path: str,
    instrument_info_path: str,
    context_file: str = "",
    max_rounds: int = 0,
    max_retries: int = 3,
    k: int = 1,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    field_relations_path: str = "",
    project_path: str = "",
    instrument_dir: str = "",
    include_source_code: bool = False,
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
    """TestPlan 主入口：测试用例池驱动的分支覆盖探索。"""
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("GSOD v5 — TestPlan 分支覆盖探索")
    logger.info("=" * 70)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "tp", operator_name)
    ckpt, ckpt_path = load_or_init_checkpoint(checkpoint_path, workdir)
    config = load_operator_config(config_path)
    branch_meta_index = load_instrumentation(instrument_info_path)
    crd_file = get_crd_file_path(config, config_dir) or ""


    if not ckpt.get("field_relations"):
        ckpt["field_relations"] = {}
    field_relations: dict = ckpt["field_relations"]
    if (
        not field_relations
        and field_relations_path
        and os.path.exists(field_relations_path)
    ):
        with open(field_relations_path, "r", encoding="utf-8") as _f:
            loaded = _json.load(_f)
        ckpt["field_relations"].update(loaded)
        logger.info(
            f"[testplan] field_relations 已从文件加载: {len(field_relations)} 条"
        )
    elif field_relations:
        logger.info(
            f"[testplan] field_relations 已从 checkpoint 加载: {len(field_relations)} 条"
        )

    declared_field_paths: set = set()
    if context_file and os.path.exists(context_file):
        _crd_fields = extract_crd_spec_fields(context_file)
        declared_field_paths = {f["path"] for f in _crd_fields}


    _profile_dir = (
        os.path.dirname(os.path.abspath(context_file)) if context_file else config_dir
    )
    constraints_data = (
        ensure_constraints(context_file, _profile_dir) if context_file else {}
    )

    gsod_context = load_gsod_context(context_file)
    env = init_cluster_env(
        config,
        config_dir,
        gsod_context,
        workdir,
        "gsod-tp",
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

    def rebuild_cluster_fn() -> dict | None:
        """拆除当前集群并重新创建，成功时返回新的 env dict，失败返回 None。"""
        nonlocal env, kubectl_client, namespace, operator_container_name, cluster_name
        logger.warning("[rebuild] 开始拆除并重建集群...")
        teardown_cluster(env, keep_cluster=False, reuse_cluster_name="")
        new_env = _setup_cluster_env(
            config,
            config_dir,
            gsod_context,
            workdir,
            "gsod-tp",
            operator_image=operator_image,
        )
        if new_env is None:
            logger.error("[rebuild] 重建集群失败")
            return None
        env = new_env
        cluster_name = env["cluster_name"]
        kubectl_client = env["kubectl_client"]
        namespace = env["namespace"]
        operator_container_name = env["operator_container_name"]
        logger.info(f"[rebuild] 集群重建完成: {cluster_name}, namespace={namespace}")
        return new_env

    if base_cr_path:
        new_cr = load_base_cr(base_cr_path, seed_cr, namespace, cr_kind, strict=True)
        if new_cr is None:
            return False
        seed_cr = new_cr
        cr_kind = seed_cr.get("kind", cr_kind)


    _db_dir = db_dir
    if not _db_dir and config_path:
        _auto_db = os.path.join(
            os.path.dirname(os.path.abspath(config_path)), "testcase_db"
        )
        _db_dir = _auto_db
        logger.info(f"[testcase_db] 自动使用 DB 目录: {_db_dir}")

    try:
        run_testplan_phase(
            ckpt=ckpt,
            ckpt_path=ckpt_path,
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            seed_cr=seed_cr,
            crd_file=crd_file,
            cr_kind=cr_kind,
            branch_meta_index=branch_meta_index,
            field_relations=field_relations or None,
            k=k,
            max_rounds=max_rounds,
            max_retries=max_retries,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            config_path=config_path,
            include_source_code=include_source_code,
            project_path=project_path,
            instrument_dir=instrument_dir,
            rebuild_cluster_fn=rebuild_cluster_fn,
            instrument_prefix=instrument_prefix,
            declared_field_paths=declared_field_paths or None,
            constraints_data=constraints_data,
            db_dir=_db_dir,
        )
    finally:
        _save_checkpoint(ckpt_path, ckpt)

        report_path = os.path.join(workdir, "testplan_report.html")
        try:
            generate_testplan_report(
                ckpt=ckpt,
                output_path=report_path,
                branch_meta_index=branch_meta_index,
            )
        except Exception as _e:
            logger.warning(f"生成报告失败: {_e}")

        teardown_cluster(env, keep_cluster, reuse_cluster_name)

    return True