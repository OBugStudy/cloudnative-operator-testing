import json
import logging
import os
from typing import List

from checkpoint.store import (
    _save_checkpoint,
    _save_json,
)
from crd.schema import (
    _extract_free_form_map_paths,
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


def run_targeted(
    config_path: str,
    instrument_info_path: str,
    targeted_config_path: str,
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
):
    """Targeted 主入口: 只探索 targeted_config 中指定的字段，输出关联报告。

    targeted_config 格式 (JSON):
    {
        "fields": [
            "spec.racks",
            "spec.podTemplateSpec.spec.containers[*].resources",
            "spec.serverVersion"
        ],
        "label": "my-test-run"   // 可选，用于 workdir 命名
    }
    """
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("GSOD v5 — Targeted 字段关联分析")
    logger.info("=" * 70)


    if not os.path.exists(targeted_config_path):
        logger.error(f"targeted config 不存在: {targeted_config_path}")
        return False
    with open(targeted_config_path, "r", encoding="utf-8") as f:
        targeted_cfg = json.load(f)

    target_paths: List[str] = targeted_cfg.get("fields", [])
    if not target_paths:
        logger.error("targeted config 中 'fields' 为空，请至少指定一个字段路径")
        return False
    label = targeted_cfg.get("label", "targeted")
    logger.info(f"目标字段数: {len(target_paths)}")
    for p in target_paths:
        logger.info(f"  - {p}")

    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, f"tgt-{label}", operator_name)

    import shutil

    shutil.copy2(targeted_config_path, os.path.join(workdir, "targeted_config.json"))

    ckpt, ckpt_path = load_or_init_checkpoint(checkpoint_path, workdir)
    config = load_operator_config(config_path)
    branch_meta_index = load_instrumentation(instrument_info_path)

    crd_file = get_crd_file_path(config, config_dir) or ""


    all_crd_fields: List[dict] = []
    if context_file and os.path.exists(context_file):
        all_crd_fields = extract_crd_spec_fields(context_file)
        logger.info(f"CRD spec 字段总数: {len(all_crd_fields)}")


    _ffm_paths: set = (
        _extract_free_form_map_paths(crd_file, cr_kind) if crd_file else set()
    )

    def _collapse_to_free_form_parent(path: str) -> str:
        """If path is a sub-key of a free-form map field, return the map field path."""
        for ffm in _ffm_paths:
            if path.startswith(ffm + ".") or path.startswith(ffm + "["):
                return ffm
        return path


    all_crd_paths = {f["path"]: f for f in all_crd_fields}
    crd_fields: List[dict] = []
    missing: List[str] = []
    for tp in target_paths:
        effective = _collapse_to_free_form_parent(tp)
        if effective != tp:
            logger.warning(
                f"字段路径 '{tp}' 是自由格式 map 字段 '{effective}' 的子键，"
                f"自动折叠为父字段进行探测"
            )
            tp = effective
        if tp in all_crd_paths:
            crd_fields.append(all_crd_paths[tp])
        else:

            missing.append(tp)
            crd_fields.append({"path": tp, "type": "unknown", "depth": tp.count(".")})


    _seen_paths: set = set()
    crd_fields_dedup: List[dict] = []
    for f in crd_fields:
        if f["path"] not in _seen_paths:
            _seen_paths.add(f["path"])
            crd_fields_dedup.append(f)
    crd_fields = crd_fields_dedup

    if missing:
        logger.warning(f"以下字段未在 CRD schema 中找到，将仍然尝试变异: {missing}")

    logger.info(f"最终探索字段数: {len(crd_fields)}")


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
        "gsod-tgt",
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

    try:
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
        )
    finally:
        _save_checkpoint(ckpt_path, ckpt)


        report_path = os.path.join(workdir, "targeted_report.html")
        try:
            generate_exploration_report(
                ckpt=ckpt,
                output_path=report_path,
                branch_meta_index=branch_meta_index,
            )
            logger.info(f"报告已保存: {report_path}")
        except Exception as e:
            logger.warning(f"生成报告失败: {e}")


        _gen_report_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "gen_report.py"
        )
        if os.path.exists(_gen_report_script):
            import subprocess as _sp2

            _ckpt_dir2 = os.path.dirname(os.path.abspath(ckpt_path))
            _report_dirs2 = {os.path.join(workdir, "report")}
            if os.path.abspath(_ckpt_dir2) != os.path.abspath(workdir):
                _report_dirs2.add(os.path.join(_ckpt_dir2, "report"))
            for _split_report_dir in sorted(_report_dirs2):
                _split_cmd = [
                    "python",
                    _gen_report_script,
                    ckpt_path,
                    "--out",
                    _split_report_dir,
                    "--instrument-info",
                    instrument_info_path,
                ]
                try:
                    _sp2.run(_split_cmd, check=True, capture_output=True, text=True)
                    logger.info(f"分页报告已保存: {_split_report_dir}/index.html")
                except Exception as e:
                    logger.warning(f"生成分页报告失败 ({_split_report_dir}): {e}")


        vis_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "visualize_checkpoint.py"
        )
        vis_report = os.path.join(workdir, "vis_report.html")
        if os.path.exists(vis_script):
            import subprocess as _sp

            vis_cmd = [
                "python",
                vis_script,
                ckpt_path,
                "--context",
                context_file,
                "--instr",
                instrument_info_path,
                "--output",
                vis_report,
            ]
            if base_cr_path:
                vis_cmd += ["--base-cr", base_cr_path]
            try:
                _sp.run(vis_cmd, check=True, capture_output=True, text=True)
                logger.info(f"可视化报告已保存: {vis_report}")
            except Exception as e:
                logger.warning(f"生成可视化报告失败: {e}")


        rel_path = os.path.join(workdir, "field_relations.json")
        _save_json(rel_path, ckpt.get("field_relations", {}))
        logger.info(f"field_relations.json 已保存: {rel_path}")

        try:
            generate_relations_html(
                ckpt.get("field_relations", {}),
                instrument_info_path,
                os.path.join(workdir, "field_relations.html"),
            )
            logger.info(
                f"field_relations.html 已保存: {os.path.join(workdir, 'field_relations.html')}"
            )
        except Exception as e:
            logger.warning(f"生成 field_relations HTML 失败: {e}")

        teardown_cluster(env, keep_cluster, reuse_cluster_name)

    return True