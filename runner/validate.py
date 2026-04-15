

import json
import logging
import os
from typing import List, Optional

from checkpoint.store import _load_checkpoint, _save_json
from cluster.apply import apply_cr_and_collect
from crd.schema import _extract_crd_required_fields, extract_crd_spec_fields
from phases.validate import run_field_validation
from report.validate import generate_validate_report
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


def _load_target_fields(
    targeted_config_path: str,
    field_args: List[str],
) -> List[str]:
    """Return the union of fields from --targeted-config and --fields args."""
    fields: List[str] = list(field_args or [])

    if targeted_config_path and os.path.exists(targeted_config_path):
        try:
            with open(targeted_config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            for fp in cfg.get("fields", []):
                if fp not in fields:
                    fields.append(fp)
            logger.info(
                f"[validate] targeted_config 加载: {targeted_config_path}  "
                f"({len(cfg.get('fields', []))} 个字段)"
            )
        except Exception as exc:
            logger.error(f"[validate] targeted_config 读取失败: {exc}")
    elif targeted_config_path:
        logger.warning(f"[validate] targeted_config 不存在: {targeted_config_path}")

    return fields


def run_validate(
    ea_checkpoint_path: str,
    config_path: str,
    instrument_info_path: str,
    targeted_config_path: str = "",
    fields: Optional[List[str]] = None,
    context_file: str = "",
    wait_sec: int = 15,
    collect_max_wait: int = 90,
    workdir_base: str = "gsod_output_v5",
    keep_cluster: bool = False,
    reuse_cluster_name: str = "",
    debug: bool = False,
    operator_image: str = "",
    cr_kind: str = "",
    instrument_prefix: str = "",
    dry_run: bool = False,
    base_cr_path: str = "",
    max_retries: int = 3,
) -> bool:
    """Validate mode main entry point.

    Args:
        ea_checkpoint_path: Path to the explore-all checkpoint.json to validate.
        config_path:         Operator config JSON.
        instrument_info_path: instrument_info.json path.
        targeted_config_path: Optional targeted_fields.json (same format as targeted mode).
        fields:              Additional field paths to validate (CLI --fields).
        context_file:        context.json (CRD schema) path.
        wait_sec:            Seconds to wait after apply.
        collect_max_wait:    Max seconds to wait for instrumentation collection.
        dry_run:             If True, skip actual cluster apply; only parse stored CRs.
    """
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("GSOD v5 — Validate 模式  (explore-all checkpoint 验证)")
    logger.info("=" * 70)


    if not ea_checkpoint_path:
        logger.error(
            "必须提供 --ea-checkpoint（或在 runner.yaml 的 validate 段中设置 ea_checkpoint）"
        )
        return False
    if not os.path.exists(ea_checkpoint_path):
        logger.error(f"checkpoint 不存在: {ea_checkpoint_path}")
        return False

    ckpt = _load_checkpoint(ea_checkpoint_path)
    if not ckpt:
        logger.error(f"checkpoint 为空或格式无效: {ea_checkpoint_path}")
        return False

    ea = ckpt.get("explore_all", {})
    mutation_log = ea.get("mutation_log", [])
    if not mutation_log:
        logger.error("checkpoint 中 explore_all.mutation_log 为空，无可验证数据")
        return False

    logger.info(
        f"checkpoint 加载完成: {len(mutation_log)} 条 mutation_log  "
        f"({len(ckpt.get('field_relations', {}))} 个字段已有关联)"
    )


    target_fields = _load_target_fields(targeted_config_path, fields or [])
    if not target_fields:

        target_fields = [e["field"] for e in mutation_log if e.get("status") == "ok"]
        logger.info(f"[validate] 未指定字段，验证全部成功字段: {len(target_fields)} 个")
    else:
        logger.info(f"[validate] 目标字段: {len(target_fields)} 个")
        for fp in target_fields:
            logger.info(f"  • {fp}")


    config_dir = os.path.dirname(os.path.abspath(config_path))
    operator_name = os.path.basename(config_dir)
    workdir = setup_runner_workdir(workdir_base, "val", operator_name)

    config = load_operator_config(config_path)
    branch_meta_index = load_instrumentation(instrument_info_path)


    declared_field_paths: Optional[set] = None
    if context_file and os.path.exists(context_file):
        crd_fields = extract_crd_spec_fields(context_file)
        declared_field_paths = {f["path"] for f in crd_fields}
        logger.info(f"CRD spec 字段: {len(declared_field_paths)} 个")


    if not cr_kind:
        for e in mutation_log:
            subs = e.get("sub_mutations", [])
            for s in subs:
                for yaml_key in ("base_cr_yaml", "mutated_cr_yaml"):
                    y = s.get(yaml_key, "")
                    if y:
                        import yaml as _yaml

                        try:
                            _cr = _yaml.safe_load(y)
                            if isinstance(_cr, dict) and _cr.get("kind"):
                                cr_kind = _cr["kind"]
                                break
                        except Exception:
                            pass
                if cr_kind:
                    break
            if cr_kind:
                break
        if cr_kind:
            logger.info(f"[validate] cr_kind 从 checkpoint 推断: {cr_kind}")

    if dry_run:
        logger.info("[validate] dry-run 模式：仅解析 CR，不连接集群")
        report = run_field_validation(
            ckpt=ckpt,
            field_paths=target_fields,
            namespace="default",
            cr_kind=cr_kind,
            kubectl_client=None,
            cluster_name="",
            operator_container_name="",
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            branch_meta_index=branch_meta_index,
            config_path=config_path,
            instrument_prefix=instrument_prefix,
            declared_field_paths=declared_field_paths,
            dry_run=True,
        )
        _write_report(report, workdir, ckpt, ea_checkpoint_path, branch_meta_index)
        return True


    gsod_context = load_gsod_context(context_file)
    env = init_cluster_env(
        config,
        config_dir,
        gsod_context,
        workdir,
        "gsod-val",
        reuse_cluster_name,
        operator_image=operator_image,
    )
    if env is None:
        return False

    cluster_name = env["cluster_name"]
    kubectl_client = env["kubectl_client"]
    namespace = env["namespace"]
    operator_container_name = env["operator_container_name"]
    cr_kind = cr_kind or env.get("seed_cr", {}).get("kind", cr_kind)
    logger.info(f"集群就绪: {cluster_name}, namespace={namespace}, cr_kind={cr_kind}")


    seed_cr: dict = env.get("seed_cr", {})
    _base_cr_path = base_cr_path or config.get("base_cr", "")
    if _base_cr_path and not os.path.isabs(_base_cr_path):
        _base_cr_path = os.path.join(config_dir, _base_cr_path)
    healthy_baseline_cr = (
        load_base_cr(_base_cr_path, seed_cr, namespace, cr_kind, strict=False)
        or seed_cr
    )


    healthy_baseline_instr: Optional[dict] = None
    logger.info("[validate] 收集健康基准 instrumentation（apply base CR）...")
    _hl_instr, _, _hl_ok, _, _cluster_dead = apply_cr_and_collect(
        kubectl_client=kubectl_client,
        namespace=namespace,
        cluster_name=cluster_name,
        input_cr=healthy_baseline_cr,
        operator_container_name=operator_container_name,
        wait_sec=wait_sec,
        collect_max_wait=collect_max_wait,
        instrument_prefix=instrument_prefix,
    )
    if _cluster_dead:
        logger.error("[validate] 集群 Pod 无法恢复，中止")
        teardown_cluster(env, keep_cluster, reuse_cluster_name)
        return False
    if _hl_ok and _hl_instr is not None:
        healthy_baseline_instr = _hl_instr
        logger.info(
            f"[validate] 健康基准收集完成: {len(healthy_baseline_instr.get('traces', []))} traces"
        )
    else:
        logger.warning("[validate] 健康基准收集失败，修正逻辑将不可用")


    crd_file = config.get("crd", "")
    if crd_file and not os.path.isabs(crd_file):
        crd_file = os.path.join(config_dir, crd_file)
    all_required_fields = (
        _extract_crd_required_fields(crd_file, cr_kind) if crd_file else []
    )

    report: dict = {}
    try:
        report = run_field_validation(
            ckpt=ckpt,
            field_paths=target_fields,
            namespace=namespace,
            cr_kind=cr_kind,
            kubectl_client=kubectl_client,
            cluster_name=cluster_name,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            branch_meta_index=branch_meta_index,
            config_path=config_path,
            instrument_prefix=instrument_prefix,
            declared_field_paths=declared_field_paths,
            dry_run=False,
            healthy_baseline_instr=healthy_baseline_instr,
            healthy_baseline_cr=healthy_baseline_cr,
            seed_cr=seed_cr,
            all_required_fields=all_required_fields,
            crd_file=crd_file,
            max_retries=max_retries,
        )
    finally:
        _write_report(report, workdir, ckpt, ea_checkpoint_path, branch_meta_index)
        teardown_cluster(env, keep_cluster, reuse_cluster_name)

    return True


def _write_report(
    report: dict,
    workdir: str,
    ckpt: dict,
    ea_checkpoint_path: str,
    branch_meta_index=None,
) -> None:
    """Write validate JSON report, HTML report, and updated field_relations."""
    report_path = os.path.join(workdir, "validate_report.json")
    try:
        _save_json(report_path, report)
        logger.info(f"验证报告已保存: {report_path}")
    except Exception as exc:
        logger.warning(f"报告写入失败: {exc}")


    ckpt_dir = os.path.dirname(os.path.abspath(ea_checkpoint_path))
    rel_path = os.path.join(ckpt_dir, "field_relations_validated.json")
    try:
        _save_json(rel_path, ckpt.get("field_relations", {}))
        logger.info(f"验证后 field_relations 已保存: {rel_path}")
    except Exception as exc:
        logger.warning(f"field_relations 写入失败: {exc}")


    rel_workdir_path = os.path.join(workdir, "field_relations.json")
    try:
        _save_json(rel_workdir_path, ckpt.get("field_relations", {}))
    except Exception:
        pass


    if report:
        html_path = os.path.join(workdir, "validate_report.html")
        try:
            generate_validate_report(
                report=report,
                output_path=html_path,
                branch_meta_index=branch_meta_index,
                ea_checkpoint_path=ea_checkpoint_path,
            )
        except Exception as exc:
            logger.warning(f"HTML 报告生成失败: {exc}")