import logging
import os
from typing import List

from checkpoint.store import _load_checkpoint, _save_json
from crd.schema import get_crd_file_path
from llm.constraints import ensure_constraints
from phases.coverage_test import run_branch_coverage_test
from report.coverage import generate_coverage_test_report
from runner.common import (
    init_cluster_env,
    load_base_cr,
    load_gsod_context,
    load_instrumentation,
    load_operator_config,
    setup_runner_workdir,
    teardown_cluster,
)

logger = logging.getLogger(__name__)


def run_coverage_test(
    config_path: str,
    instrument_info_path: str,
    checkpoint_path: str,
    targets_spec: List[dict],
    workdir_base: str = "gsod_output_v5",
    wait_sec: int = 15,
    collect_max_wait: int = 30,
    max_retries: int = 3,
    keep_cluster: bool = False,
    reuse_cluster_name: str = "",
    base_cr_path: str = "",
    project_path: str = "",
    instrument_dir: str = "",
    include_source_code: bool = False,
    debug: bool = False,
    context_file: str = "",
    operator_image: str = "",
    cr_kind: str = "",
    instrument_prefix: str = "",
) -> bool:
    """Entry point for coverage-test mode."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("GSOD v5 — Coverage Test Mode")
    logger.info("=" * 70)

    if not checkpoint_path or not os.path.exists(checkpoint_path):
        logger.error(f"Checkpoint 文件不存在: {checkpoint_path}")
        return False
    if not targets_spec:
        logger.error("未指定测试目标 (--targets)")
        return False

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "cov", operator_name)

    ckpt = _load_checkpoint(checkpoint_path)
    logger.info(f"Checkpoint 已加载: {checkpoint_path}")

    config = load_operator_config(config_path)
    branch_meta_index = load_instrumentation(instrument_info_path)
    crd_file = get_crd_file_path(config, config_dir) or ""


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
        "gsod-cov",
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

    if base_cr_path:
        new_cr = load_base_cr(base_cr_path, seed_cr, namespace, cr_kind, strict=False)
        if new_cr is not seed_cr:
            seed_cr = new_cr
            cr_kind = seed_cr.get("kind", cr_kind)

    try:
        results = run_branch_coverage_test(
            targets=targets_spec,
            ckpt=ckpt,
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            seed_cr=seed_cr,
            crd_file=crd_file,
            cr_kind=cr_kind,
            branch_meta_index=branch_meta_index,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            max_retries=max_retries,
            project_path=project_path,
            instrument_dir=instrument_dir,
            include_source_code=include_source_code,
            instrument_prefix=instrument_prefix,
            constraints_data=constraints_data,
        )
    finally:
        teardown_cluster(env, keep_cluster, reuse_cluster_name)


    results_path = os.path.join(workdir, "coverage_test_results.json")
    _save_json(results_path, results)
    logger.info(f"结果 JSON 已保存: {results_path}")


    report_path = os.path.join(workdir, "coverage_test_report.html")
    generate_coverage_test_report(
        results=results,
        targets=targets_spec,
        branch_meta_index=branch_meta_index,
        output_path=report_path,
    )

    n_ok = sum(1 for r in results if r["success"])
    n_fail = len(results) - n_ok
    logger.info(f"\n[coverage-test] 完成: {n_ok}/{len(results)} 目标覆盖成功")
    logger.info(f"报告: {report_path}")
    return n_fail == 0