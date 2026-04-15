import logging
import os
from typing import List

from checkpoint.store import (
    _save_checkpoint,
    _save_json,
)
from crd.schema import (
    extract_crd_spec_fields,
    get_crd_file_path,
)
from llm.constraints import ensure_constraints
from phases.explore_all import run_full_field_exploration
from relations.html import generate_relations_html
from report.explore_all import generate_exploration_report
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


def run_explore_all(
    config_path: str,
    instrument_info_path: str,
    context_file: str = "",
    max_retries: int = 3,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    workdir_base: str = "gsod_output_v5",
    keep_cluster: bool = False,
    reuse_cluster_name: str = "",
    checkpoint_path: str = "",
    debug: bool = False,
    base_cr_path: str = "",
    no_llm: bool = False,
    operator_image: str = "",
    cr_kind: str = "",
    instrument_prefix: str = "",
    project_path: str = "",
    instrument_dir: str = "",
    db_dir: str = "",
):
    """Explore-All 主入口: 对全部 CRD 字段建立 branch 关联映射。"""
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("GSOD v5 — Explore-All 全量字段关联分析")
    logger.info("=" * 70)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "ea", operator_name)
    ckpt, ckpt_path = load_or_init_checkpoint(checkpoint_path, workdir)
    config = load_operator_config(config_path)
    branch_meta_index = load_instrumentation(instrument_info_path)

    crd_file = get_crd_file_path(config, config_dir) or ""
    crd_fields: List[dict] = []
    if context_file and os.path.exists(context_file):
        crd_fields = extract_crd_spec_fields(context_file)
        logger.info(f"CRD spec 字段: {len(crd_fields)} 个")


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
        "gsod-ea",
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


    acto_input_model = None
    if no_llm:
        from run_acto import _build_flat_plan, _load_crd_body_from_context

        _crd_body = _load_crd_body_from_context(context_file)
        if _crd_body is None:
            logger.error("[no-llm] 无法加载 CRD body，退出")
            teardown_cluster(env, keep_cluster, reuse_cluster_name)
            return False
        try:
            _, acto_input_model = _build_flat_plan(config, _crd_body, seed_cr)
            logger.info("[no-llm] Acto 输入模型已构建")
        except Exception as _e:
            logger.error(f"[no-llm] Acto 输入模型构建失败: {_e}", exc_info=True)
            teardown_cluster(env, keep_cluster, reuse_cluster_name)
            return False

    _interrupted = False
    try:

        _db_dir = db_dir
        if not _db_dir and config_path:
            _auto_db = os.path.join(
                os.path.dirname(os.path.abspath(config_path)), "testcase_db"
            )
            _db_dir = _auto_db
            logger.info(f"[testcase_db] 自动使用 DB 目录: {_db_dir}")

        run_full_field_exploration(
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
            max_retries=max_retries,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            branch_meta_index=branch_meta_index,
            config_path=config_path,
            no_llm=no_llm,
            acto_input_model=acto_input_model,
            acto_seed_cr=seed_cr,
            instrument_prefix=instrument_prefix,
            constraints_data=constraints_data,
            project_path=project_path,
            instrument_dir=instrument_dir,
            db_dir=_db_dir,
        )
    except KeyboardInterrupt:
        _interrupted = True
        logger.warning("\n[中断] 收到 Ctrl+C，正在安全保存 checkpoint...")


    try:
        _save_checkpoint(ckpt_path, ckpt)
        logger.info(f"[checkpoint] 已保存: {ckpt_path}")
    except Exception as _e:
        logger.error(f"[checkpoint] 保存失败: {_e}")

    rel_dir = os.path.dirname(os.path.abspath(instrument_info_path))
    rel_path = os.path.join(rel_dir, "field_relations.json")
    try:
        _save_json(rel_path, ckpt.get("field_relations", {}))
        logger.info(f"[checkpoint] field_relations.json 已保存: {rel_path}")
    except Exception as _e:
        logger.error(f"[checkpoint] field_relations.json 保存失败: {_e}")

    if _interrupted:
        logger.warning(
            "\n" + "=" * 60 + "\n"
            "已中断。断点续传信息：\n"
            f"  Checkpoint : {ckpt_path}\n"
            f"  field_relations: {rel_path}\n"
            "重新运行时使用相同的 --checkpoint 参数即可续传。\n"
            "集群未被清理（如需手动清理请运行 kind delete cluster）。\n" + "=" * 60
        )
        return False


    report_path = os.path.join(workdir, "explore_all_report.html")
    try:
        generate_exploration_report(
            ckpt=ckpt,
            output_path=report_path,
            branch_meta_index=branch_meta_index,
        )
    except Exception as e:
        logger.warning(f"生成报告失败: {e}")


    _gen_report_script = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "gen_report.py"
    )
    if os.path.exists(_gen_report_script):
        import subprocess as _sp_ea

        _ckpt_dir = os.path.dirname(os.path.abspath(ckpt_path))
        _report_dirs = {os.path.join(workdir, "report")}
        if os.path.abspath(_ckpt_dir) != os.path.abspath(workdir):
            _report_dirs.add(os.path.join(_ckpt_dir, "report"))
        for _split_report_dir in sorted(_report_dirs):
            _split_cmd_ea = [
                "python",
                _gen_report_script,
                ckpt_path,
                "--out",
                _split_report_dir,
                "--instrument-info",
                instrument_info_path,
            ]
            try:
                _sp_ea.run(_split_cmd_ea, check=True, capture_output=True, text=True)
                logger.info(f"分页报告已保存: {_split_report_dir}/index.html")
            except Exception as e:
                logger.warning(f"生成分页报告失败 ({_split_report_dir}): {e}")

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