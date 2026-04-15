import logging
import os

import yaml

from checkpoint.store import (
    _save_checkpoint,
    _save_json,
)
from crd.schema import (
    extract_crd_spec_fields,
    get_crd_file_path,
)
from phases.phase1 import run_association_analysis_phase
from phases.phase2 import run_coverage_generation_phase
from relations.html import generate_relations_html
from report.phase1 import generate_pipeline_report
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


def run_gsod_pipeline(
    config_path: str,
    instrument_info_path: str,
    context_file: str = "",

    num_fields: int = 10,

    k: int = 1,
    max_retries: int = 3,
    max_combos: int = 0,
    project_path: str = "",
    instrument_dir: str = "",

    wait_sec: int = 15,
    collect_max_wait: int = 0,
    workdir_base: str = "gsod_output_v5",
    keep_cluster: bool = False,
    reuse_cluster_name: str = "",
    checkpoint_path: str = "",
    debug: bool = False,
    base_cr_path: str = "",
    include_source_code: bool = False,
    operator_image: str = "",
    cr_kind: str = "",
    instrument_prefix: str = "",
):
    """GSOD v5 主入口: Phase 1 关联分析 → Phase 2 测试计划生成。"""
    logger.info("=" * 70)
    logger.info("GSOD v5 — 反馈驱动测试引擎")
    logger.info("=" * 70)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "v5", operator_name)
    ckpt, ckpt_path = load_or_init_checkpoint(checkpoint_path, workdir)
    config = load_operator_config(config_path)
    branch_meta_index = load_instrumentation(instrument_info_path)
    all_branch_indices = sorted(branch_meta_index.keys())

    crd_file = get_crd_file_path(config, config_dir) or ""
    crd_fields = []
    if context_file and os.path.exists(context_file):
        crd_fields = extract_crd_spec_fields(context_file)
        logger.info(f"CRD spec 字段: {len(crd_fields)} 个")

    gsod_context = load_gsod_context(context_file)
    env = init_cluster_env(
        config,
        config_dir,
        gsod_context,
        workdir,
        "gsod-v5",
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

    if base_cr_path:
        new_cr = load_base_cr(base_cr_path, seed_cr, namespace, cr_kind, strict=True)
        if new_cr is None:
            return False
        seed_cr = new_cr
        cr_kind = seed_cr.get("kind", cr_kind)


    if not ckpt.get("current_cr_yaml"):
        ckpt["current_cr_yaml"] = yaml.dump(seed_cr)

    try:

        run_association_analysis_phase(
            ckpt=ckpt,
            ckpt_path=ckpt_path,
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            seed_cr=seed_cr,
            crd_file=crd_file,
            cr_kind=cr_kind,
            crd_fields=crd_fields,
            num_fields=num_fields,
            max_retries=max_retries,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            branch_meta_index=branch_meta_index,
            instrument_prefix=instrument_prefix,
        )


        run_coverage_generation_phase(
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
            all_branch_indices=all_branch_indices,
            k=k,
            max_retries=max_retries,
            max_combos=max_combos,
            project_path=project_path,
            instrument_dir=instrument_dir,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            config_path=config_path,
            include_source_code=include_source_code,
            instrument_prefix=instrument_prefix,
        )

    finally:

        _save_checkpoint(ckpt_path, ckpt)


        report_path = os.path.join(workdir, "report.html")
        try:
            generate_pipeline_report(
                ckpt=ckpt,
                branch_meta_index=branch_meta_index,
                instrument_info_path=instrument_info_path,
                output_path=report_path,
            )
        except Exception as e:
            logger.warning(f"生成报告失败: {e}")


        rel_dir = os.path.dirname(os.path.abspath(instrument_info_path))
        rel_path = os.path.join(rel_dir, "field_relations.json")
        _save_json(rel_path, ckpt.get("field_relations", {}))
        logger.info(f"field_relations.json 已保存: {rel_path}")

        try:
            generate_relations_html(
                ckpt.get("field_relations", {}),
                instrument_info_path,
                os.path.join(rel_dir, "field_relations.html"),
            )
        except Exception as e:
            logger.warning(f"生成 field_relations HTML 失败: {e}")

        teardown_cluster(env, keep_cluster, reuse_cluster_name)

    return True