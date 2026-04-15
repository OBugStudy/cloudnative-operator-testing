

import logging
import os
from typing import List

import yaml

from checkpoint.store import (
    _load_checkpoint,
)
from core.rich_logger import setup_rich_logging, stop_rich_logging
from instrumentation.diff import (
    _build_branch_index,
)
from report.explore_all import generate_exploration_report
from report.phase1 import generate_pipeline_report
from report.testplan import generate_testplan_report
from runner.coverage import run_coverage_test
from runner.e2e import run_e2e
from runner.explore_all import run_explore_all
from runner.fault import run_fault
from runner.probe import run_testplan_probe
from runner.targeted import run_targeted
from runner.testplan import run_testplan
from runner.validate import run_validate
from runner.workflow import run_gsod_pipeline

logger = logging.getLogger(__name__)


def _load_profile(profile_path: str) -> dict:
    """Load a runner.yaml profile and return a flat dict of key→value defaults.

    Keys use underscores (matching argparse dest).  Values from the ``common``
    section apply to all modes; mode-specific sections override them for that
    mode only.  The returned dict is keyed by (mode_or_None, dest).
    Example returned structure::

        {
            (None, "config"): "data/CassOp/config-only.json",
            ("targeted", "targeted_config"): "data/CassOp/targeted_fields.json",
        }
    """
    if not profile_path or not os.path.exists(profile_path):
        return {}
    try:
        with open(profile_path, "r", encoding="utf-8") as _f:
            data = yaml.safe_load(_f) or {}
    except Exception as exc:
        logger.warning(f"[profile] 读取失败 {profile_path}: {exc}")
        return {}

    out: dict = {}
    for key, val in (data.get("common") or {}).items():
        out[(None, str(key))] = val
    for mode in (
        "run",
        "explore-all",
        "targeted",
        "report",
        "explore-all-report",
        "coverage-test",
        "testplan",
        "testplan-report",
        "testplan-probe",
        "fault",
        "preflight",
    ):
        for key, val in (data.get(mode) or {}).items():
            out[(mode, str(key))] = val
    if not out:
        logger.warning(
            f"[profile] 文件已加载但未读取到任何配置键: {profile_path}\n"
            f"  请确认文件格式正确（应为 YAML，含 common:/testplan: 等节）。\n"
            f"  提示：profile 文件应为 .yaml，而非 .py 等其他格式。"
        )
    return out


def main():
    import argparse


    _pre = argparse.ArgumentParser(add_help=False)
    _pre.add_argument(
        "--profile", default="", metavar="PATH", help="runner.yaml profile 路径"
    )
    _pre_args, _remaining = _pre.parse_known_args()
    _profile = _load_profile(_pre_args.profile)

    def _profile_default(mode: str, key: str, fallback):
        """Return profile value (mode-specific beats common) or fallback."""
        if (mode, key) in _profile:
            return _profile[(mode, key)]
        if (None, key) in _profile:
            return _profile[(None, key)]
        return fallback

    parser = argparse.ArgumentParser(
        description="GSOD v5 — 反馈驱动测试引擎",
    )
    parser.add_argument(
        "--profile",
        default="",
        metavar="PATH",
        help="runner.yaml profile 路径（预设通用参数）",
    )
    subparsers = parser.add_subparsers(dest="mode")


    def _add_common(p, mode=""):
        p.add_argument(
            "--config",
            default=_profile_default(mode, "config", "data/CassOp/config-only.json"),
            help="Operator 配置文件路径",
        )
        p.add_argument(
            "--instrument-info",
            default=_profile_default(mode, "instrument_info", "./instrument_info.json"),
            help="插桩信息文件路径",
        )
        p.add_argument(
            "--context",
            default=_profile_default(mode, "context", "data/CassOp/context.json"),
            help="context.json 路径",
        )
        p.add_argument(
            "--wait-sec",
            type=int,
            default=_profile_default(mode, "wait_sec", 15),
            help="apply CR 后等待秒数 (默认 15)",
        )
        p.add_argument(
            "--collect-max-wait",
            type=int,
            default=_profile_default(mode, "collect_max_wait", 90),
            help="收集器最大等待秒数，0=无限 (默认 90)",
        )
        p.add_argument(
            "--workdir-base",
            default=_profile_default(mode, "workdir_base", "gsod_output_v5"),
            help="输出目录父目录",
        )
        p.add_argument("--debug", action="store_true")
        _keep_default = _profile_default(mode, "keep_cluster", False)
        if _keep_default:
            p.set_defaults(keep_cluster=True)
        p.add_argument("--keep-cluster", action="store_true", help="测试后保留集群")
        p.add_argument(
            "--reuse-cluster",
            default=_profile_default(mode, "reuse_cluster", ""),
            metavar="NAME",
            help="复用已有集群",
        )
        p.add_argument(
            "--checkpoint",
            default=_profile_default(mode, "checkpoint", ""),
            metavar="PATH",
            help="断点续传文件路径",
        )
        p.add_argument(
            "--operator-image",
            default=_profile_default(mode, "operator_image", ""),
            metavar="IMAGE",
            help="需要预加载到 Kind 集群的 Operator 镜像（如 k8ssandra/cass-operator:v1.22.1-inst）",
        )
        p.add_argument(
            "--cr-kind",
            default=_profile_default(mode, "cr_kind", ""),
            metavar="KIND",
            help="CR 真实 Kind，用于 kubectl 操作（如 CassandraDatacenter）",
        )
        p.add_argument(
            "--instrument-prefix",
            default=_profile_default(mode, "instrument_prefix", ""),
            metavar="PREFIX",
            help="operator 插桩前缀，用于 data-service trace 过滤（如 CassandraDatacenterReconciler）",
        )


    p_run = subparsers.add_parser(
        "run",
        help="端到端自动化测试：加载 testplan 测试用例，逐个 apply CR 并检查结果",
    )
    _add_common(p_run, "run")
    p_run.add_argument(
        "--testplan-checkpoint",
        default=_profile_default("run", "testplan_checkpoint", ""),
        metavar="PATH",
        help="testplan checkpoint.json 路径（含测试用例，必填）",
    )
    p_run.add_argument(
        "--max-rounds",
        type=int,
        default=_profile_default("run", "max_rounds", 0),
        metavar="N",
        help="最大测试轮数，0=不限（默认 0，执行全部测试用例）",
    )
    p_run.add_argument(
        "--base-cr",
        default=_profile_default("run", "base_cr", ""),
        metavar="PATH",
        help="完全体 base CR YAML 路径，用于替代默认 seed CR",
    )


    p_run_legacy = subparsers.add_parser(
        "run-legacy",
        help="[旧版] 执行完整流程 (Phase 1 关联分析 + Phase 2 测试计划生成)",
    )
    _add_common(p_run_legacy, "run")
    p_run_legacy.add_argument(
        "--num-fields",
        type=int,
        default=_profile_default("run", "num_fields", 10),
        metavar="N",
        help="Phase 1 变异字段数 (默认 10)",
    )
    p_run_legacy.add_argument(
        "--k",
        type=int,
        default=_profile_default("run", "k", 1),
        metavar="K",
        help="k-branch 组合度 (默认 1)",
    )
    p_run_legacy.add_argument(
        "--max-retries",
        type=int,
        default=_profile_default("run", "max_retries", 3),
        metavar="N",
    )
    p_run_legacy.add_argument(
        "--max-combos",
        type=int,
        default=_profile_default("run", "max_combos", 0),
        metavar="N",
    )
    p_run_legacy.add_argument(
        "--project-path",
        default=_profile_default("run", "project_path", ""),
        metavar="PATH",
    )
    p_run_legacy.add_argument(
        "--instrument-dir",
        default=_profile_default("run", "instrument_dir", ""),
        metavar="DIR",
    )
    p_run_legacy.add_argument(
        "--base-cr",
        default=_profile_default("run", "base_cr", ""),
        metavar="PATH",
    )
    p_run_legacy.add_argument(
        "--include-source-code",
        action="store_true",
        default=_profile_default("run", "include_source_code", False),
    )


    p_report = subparsers.add_parser("report", help="从 checkpoint 生成 HTML 报告")
    p_report.add_argument(
        "--checkpoint",
        default=_profile_default("report", "checkpoint", ""),
        metavar="PATH",
        help="checkpoint.json 路径",
    )
    p_report.add_argument(
        "--instrument-info",
        default=_profile_default("report", "instrument_info", "./instrument_info.json"),
        help="插桩信息文件路径",
    )
    p_report.add_argument(
        "--output",
        default=_profile_default("report", "output", "report.html"),
        metavar="PATH",
        help="输出 HTML 路径",
    )


    p_ea = subparsers.add_parser(
        "explore-all", help="对全部 CRD 字段建立 branch 关联（支持断点续传）"
    )
    _add_common(p_ea, "explore-all")
    p_ea.add_argument(
        "--max-retries",
        type=int,
        default=_profile_default("explore-all", "max_retries", 3),
        metavar="N",
        help="每个字段 LLM 最大尝试次数 (默认 3)",
    )
    p_ea.add_argument(
        "--base-cr",
        default=_profile_default("explore-all", "base_cr", ""),
        metavar="PATH",
        help="完全体 base CR YAML 路径（替代 seed CR）",
    )
    p_ea.add_argument(
        "--no-llm",
        action="store_true",
        default=bool(_profile_default("explore-all", "no_llm", False)),
        help="不调用 LLM，改用 acto 字段变异规则探索 field→branch 关联",
    )
    p_ea.add_argument(
        "--project-path",
        default=_profile_default("explore-all", "project_path", ""),
        metavar="PATH",
        help="operator 源码根目录（运行时语义校验用）",
    )
    p_ea.add_argument(
        "--instrument-dir",
        default=_profile_default("explore-all", "instrument_dir", ""),
        metavar="DIR",
        help="插桩目录（运行时语义校验用）",
    )
    p_ea.add_argument(
        "--db-dir",
        default=_profile_default("explore-all", "db_dir", ""),
        metavar="DIR",
        help="测试用例数据库目录（默认与 config 同目录下的 testcase_db/）",
    )


    p_probe = subparsers.add_parser(
        "testplan-probe",
        help="从 checkpoint 指定 testcase+target 单次运行并输出详细覆盖诊断信息",
    )
    p_probe.add_argument(
        "--checkpoint",
        required=True,
        metavar="PATH",
        help="testplan checkpoint.json 路径",
    )
    p_probe.add_argument(
        "--config",
        default=_profile_default(
            "testplan-probe", "config", "data/CassOp/config-only.json"
        ),
        metavar="PATH",
        help="Operator 配置文件",
    )
    p_probe.add_argument(
        "--instrument-info",
        default=_profile_default("testplan-probe", "instrument_info", ""),
        metavar="PATH",
        help="instrument_info.json 路径",
    )
    p_probe.add_argument(
        "--testcase-id",
        required=True,
        metavar="ID",
        help="测试用例 ID，如 '3'",
    )
    p_probe.add_argument(
        "--target-key",
        required=True,
        metavar="KEY",
        help="目标键，如 '42_T' 或 '42_F'",
    )
    p_probe.add_argument(
        "--context",
        default=_profile_default("testplan-probe", "context", ""),
        metavar="PATH",
        help="context.json 路径",
    )
    p_probe.add_argument(
        "--field-relations",
        default=_profile_default("testplan-probe", "field_relations", ""),
        metavar="PATH",
        help="field_relations.json 路径（可选）",
    )
    p_probe.add_argument(
        "--project-path",
        default=_profile_default("testplan-probe", "project_path", ""),
        metavar="PATH",
        help="operator 源码根目录",
    )
    p_probe.add_argument(
        "--instrument-dir",
        default=_profile_default("testplan-probe", "instrument_dir", ""),
        metavar="PATH",
        help="插桨目录",
    )
    p_probe.add_argument(
        "--include-source-code",
        action="store_true",
        default=_profile_default("testplan-probe", "include_source_code", False),
        help="在 LLM prompt 中附带源码上下文",
    )
    p_probe.add_argument(
        "--max-retries",
        type=int,
        default=_profile_default("testplan-probe", "max_retries", 3),
        metavar="N",
        help="LLM 最大尝试次数 (默认 3)",
    )
    p_probe.add_argument(
        "--wait-sec",
        type=int,
        default=_profile_default("testplan-probe", "wait_sec", 15),
        metavar="N",
    )
    p_probe.add_argument(
        "--collect-max-wait",
        type=int,
        default=_profile_default("testplan-probe", "collect_max_wait", 90),
        metavar="N",
    )
    p_probe.add_argument(
        "--reuse-cluster",
        default=_profile_default("testplan-probe", "reuse_cluster", ""),
        metavar="NAME",
        help="复用已有集群",
    )
    p_probe.add_argument("--keep-cluster", action="store_true")
    p_probe.add_argument(
        "--base-cr",
        default=_profile_default("testplan-probe", "base_cr", ""),
        metavar="PATH",
    )
    p_probe.add_argument(
        "--no-llm",
        action="store_true",
        help="跳过 LLM，直接用 testcase 原始 CR apply",
    )
    p_probe.add_argument(
        "--workdir-base",
        default=_profile_default("testplan-probe", "workdir_base", "gsod_output_v5"),
    )
    p_probe.add_argument("--debug", action="store_true")


    p_tpr = subparsers.add_parser(
        "testplan-report", help="从 checkpoint 生成 testplan 覆盖报告"
    )
    p_tpr.add_argument(
        "--checkpoint",
        default=_profile_default("testplan-report", "checkpoint", ""),
        metavar="PATH",
        help="checkpoint.json 路径",
    )
    p_tpr.add_argument(
        "--output",
        default=_profile_default("testplan-report", "output", "testplan_report.html"),
        metavar="PATH",
        help="输出 HTML 路径",
    )
    p_tpr.add_argument(
        "--instrument-info",
        default=_profile_default("testplan-report", "instrument_info", ""),
        metavar="PATH",
        help="instrument_info.json 路径（可选，用于分支元数据）",
    )


    p_ear = subparsers.add_parser(
        "explore-all-report", help="从 checkpoint 生成 explore-all 关联分析报告"
    )
    p_ear.add_argument(
        "--checkpoint",
        default=_profile_default("explore-all-report", "checkpoint", ""),
        metavar="PATH",
        help="checkpoint.json 路径",
    )
    p_ear.add_argument(
        "--output",
        default=_profile_default(
            "explore-all-report", "output", "explore_all_report.html"
        ),
        metavar="PATH",
        help="输出 HTML 路径",
    )


    p_cov = subparsers.add_parser(
        "coverage-test",
        help="从 checkpoint 读取 field_relations，对指定 branch 目标生成 CR 并验证覆盖",
    )
    _add_common(p_cov, "coverage-test")
    p_cov.add_argument(
        "--targets",
        required=True,
        metavar="SPEC",
        help="测试目标，格式: '<bi>:<true|false>[,<bi>:<true|false>...]'"
        "  例如: '42:true,43:false,100:true'",
    )
    p_cov.add_argument(
        "--max-retries",
        type=int,
        default=_profile_default("coverage-test", "max_retries", 3),
        metavar="N",
        help="每个目标 LLM 最大尝试次数 (默认 3)",
    )
    p_cov.add_argument(
        "--base-cr",
        default=_profile_default("coverage-test", "base_cr", ""),
        metavar="PATH",
        help="完全体 base CR YAML 路径",
    )
    p_cov.add_argument(
        "--project-path",
        default=_profile_default("coverage-test", "project_path", ""),
        metavar="PATH",
        help="operator 源码根目录（用于获取源码上下文）",
    )
    p_cov.add_argument(
        "--instrument-dir",
        default=_profile_default("coverage-test", "instrument_dir", ""),
        metavar="DIR",
        help="插桩目录（用于获取源码上下文）",
    )
    p_cov.add_argument(
        "--include-source-code",
        action="store_true",
        default=_profile_default("coverage-test", "include_source_code", False),
        help="在 LLM prompt 中附带源码上下文（默认不附带）",
    )


    p_tgt = subparsers.add_parser(
        "targeted", help="按指定字段列表运行关联分析并输出报告"
    )
    _add_common(p_tgt, "targeted")
    p_tgt.add_argument(
        "--targeted-config",
        default=_profile_default("targeted", "targeted_config", ""),
        metavar="PATH",
        help="目标字段配置 JSON 文件路径（含 fields 数组）",
    )
    p_tgt.add_argument(
        "--max-retries",
        type=int,
        default=_profile_default("targeted", "max_retries", 3),
        metavar="N",
        help="每个字段 LLM 最大尝试次数 (默认 3)",
    )
    p_tgt.add_argument(
        "--base-cr",
        default=_profile_default("targeted", "base_cr", ""),
        metavar="PATH",
        help="完全体 base CR YAML 路径（替代 seed CR）",
    )
    p_tgt.add_argument(
        "--no-llm",
        action="store_true",
        default=bool(_profile_default("targeted", "no_llm", False)),
        help="不调用 LLM，改用 acto 字段变异规则探索 field→branch 关联",
    )
    p_tgt.add_argument(
        "--project-path",
        default=_profile_default("targeted", "project_path", ""),
        metavar="PATH",
        help="operator 源码根目录（运行时语义校验用）",
    )
    p_tgt.add_argument(
        "--instrument-dir",
        default=_profile_default("targeted", "instrument_dir", ""),
        metavar="DIR",
        help="插桨目录（运行时语义校验用）",
    )


    p_val = subparsers.add_parser(
        "validate",
        help="从 explore-all checkpoint 重放指定字段的变异，验证 field→branch 关联",
    )
    p_val.add_argument(
        "--ea-checkpoint",
        default=_profile_default("validate", "ea_checkpoint", ""),
        metavar="PATH",
        help="explore-all checkpoint.json 路径",
    )
    p_val.add_argument(
        "--config",
        default=_profile_default("validate", "config", "data/CassOp/config-only.json"),
        help="Operator 配置文件路径",
    )
    p_val.add_argument(
        "--instrument-info",
        default=_profile_default(
            "validate", "instrument_info", "./instrument_info.json"
        ),
        help="插桩信息文件路径",
    )
    p_val.add_argument(
        "--context",
        default=_profile_default("validate", "context", "data/CassOp/context.json"),
        help="context.json 路径（CRD schema）",
    )
    p_val.add_argument(
        "--targeted-config",
        default=_profile_default("validate", "targeted_config", ""),
        metavar="PATH",
        help="targeted_fields.json 路径（与 targeted 模式格式相同）",
    )
    p_val.add_argument(
        "--fields",
        nargs="+",
        default=[],
        metavar="FIELD",
        help="额外指定的字段路径（可多个，空格分隔）",
    )
    p_val.add_argument(
        "--wait-sec",
        type=int,
        default=_profile_default("validate", "wait_sec", 15),
        help="apply CR 后等待秒数 (默认 15)",
    )
    p_val.add_argument(
        "--collect-max-wait",
        type=int,
        default=_profile_default("validate", "collect_max_wait", 90),
        help="收集器最大等待秒数 (默认 90)",
    )
    p_val.add_argument(
        "--workdir-base",
        default=_profile_default("validate", "workdir_base", "gsod_output_v5"),
        help="输出目录父目录",
    )
    p_val.add_argument("--debug", action="store_true")
    p_val.add_argument("--keep-cluster", action="store_true", help="测试后保留集群")
    p_val.add_argument(
        "--reuse-cluster",
        default=_profile_default("validate", "reuse_cluster", ""),
        metavar="NAME",
        help="复用已有集群",
    )
    p_val.add_argument(
        "--operator-image",
        default=_profile_default("validate", "operator_image", ""),
        metavar="IMAGE",
        help="需要预加载到 Kind 集群的 Operator 镜像",
    )
    p_val.add_argument(
        "--cr-kind",
        default=_profile_default("validate", "cr_kind", ""),
        metavar="KIND",
        help="CR Kind（如 CassandraDatacenter）",
    )
    p_val.add_argument(
        "--instrument-prefix",
        default=_profile_default("validate", "instrument_prefix", ""),
        metavar="PREFIX",
        help="插桩数据查询前缀",
    )
    p_val.add_argument(
        "--dry-run",
        action="store_true",
        help="仅解析存储的 CR，不连接集群（调试用）",
    )


    p_pre = subparsers.add_parser(
        "preflight",
        help="从 CRD 生成/更新字段约束文件 constraint.json（存放于 context.json 同目录）",
    )
    p_pre.add_argument(
        "--context",
        default=_profile_default("preflight", "context", "data/CassOp/context.json"),
        metavar="PATH",
        help="context.json 路径（含 CRD 信息）",
    )
    p_pre.add_argument(
        "--force",
        action="store_true",
        help="强制重新生成，即使 constraint.json 已存在",
    )


    p_fault = subparsers.add_parser(
        "fault",
        help="故障注入测试：以 testplan checkpoint 的测试用例为输入，每轮随机注入故障并验证程序正常运行",
    )
    _add_common(p_fault, "fault")
    p_fault.add_argument(
        "--testplan-checkpoint",
        required=True,
        metavar="PATH",
        help="testplan checkpoint.json 路径（含测试用例）",
    )
    p_fault.add_argument(
        "--fault-types",
        default=_profile_default("fault", "fault_types", "crash"),
        metavar="TYPES",
        help="故障类型，逗号分隔：crash,reconnect,delay （默认: crash）",
    )
    p_fault.add_argument(
        "--fault-manager-url",
        default=_profile_default("fault", "fault_manager_url", ""),
        metavar="URL",
        help="Fault Manager 服务地址，用于 reconnect/delay 故障（如 http://localhost:8080）",
    )
    p_fault.add_argument(
        "--max-rounds",
        type=int,
        default=_profile_default("fault", "max_rounds", 0),
        metavar="N",
        help="最大测试轮数，0=不限制",
    )
    p_fault.add_argument(
        "--base-cr",
        default=_profile_default("fault", "base_cr", ""),
        metavar="PATH",
        help="完全体 base CR YAML 路径",
    )


    p_plan = subparsers.add_parser(
        "testplan",
        help="测试用例池驱动的分支覆盖探索",
    )
    _add_common(p_plan, "testplan")
    p_plan.add_argument(
        "--max-rounds",
        type=int,
        default=_profile_default("testplan", "max_rounds", 0),
        metavar="N",
        help="主循环最大轮数，0 = 不限制 (默认 0)",
    )
    p_plan.add_argument(
        "--max-retries",
        type=int,
        default=_profile_default("testplan", "max_retries", 3),
        metavar="N",
        help="每个目标 LLM flip 最大尝试次数 (默认 3)",
    )
    p_plan.add_argument(
        "--field-relations",
        default=_profile_default("testplan", "field_relations", ""),
        metavar="PATH",
        help="field_relations.json 路径（可选，优先于 checkpoint 中的值）",
    )
    p_plan.add_argument(
        "--project-path",
        default=_profile_default("testplan", "project_path", ""),
        metavar="PATH",
        help="operator 源码根目录（LLM 源码上下文用）",
    )
    p_plan.add_argument(
        "--instrument-dir",
        default=_profile_default("testplan", "instrument_dir", ""),
        metavar="PATH",
        help="插桩目录（LLM 源码上下文用）",
    )
    p_plan.add_argument(
        "--include-source-code",
        action="store_true",
        default=_profile_default("testplan", "include_source_code", False),
        help="在 LLM prompt 中附带 Go 源码上下文",
    )
    p_plan.add_argument(
        "--base-cr",
        default=_profile_default("testplan", "base_cr", ""),
        metavar="PATH",
        help="完全体 base CR YAML 路径（替代 seed CR）",
    )
    p_plan.add_argument(
        "--k",
        type=int,
        default=_profile_default("testplan", "k", 1),
        metavar="N",
        help="目标分支组合数: 1=单分支目标, 2=两分支组合目标, 3=三分支组合目标 (默认 1)",
    )
    p_plan.add_argument(
        "--db-dir",
        default=_profile_default("testplan", "db_dir", ""),
        metavar="DIR",
        help="测试用例数据库目录（默认与 config 同目录下的 testcase_db/）",
    )

    p_fault.add_argument(
        "--db-dir",
        default=_profile_default("fault", "db_dir", ""),
        metavar="DIR",
        help="测试用例数据库目录（默认与 config 同目录下的 testcase_db/）",
    )

    args = parser.parse_args()


    _operator = ""
    try:
        import os as _os

        _operator = (
            _os.path.basename(_os.path.dirname(_os.path.abspath(args.config)))
            if hasattr(args, "config") and args.config
            else ""
        )
    except Exception:
        pass
    setup_rich_logging(
        mode=args.mode or "",
        operator=_operator,
        log_level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO,
    )

    try:
        _run_main(args, parser)
    finally:
        stop_rich_logging()


def _run_main(args, parser):
    """Dispatch to the appropriate runner based on the selected mode."""
    if args.mode == "run":
        run_e2e(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            testplan_checkpoint_path=args.testplan_checkpoint,
            context_file=args.context,
            max_rounds=args.max_rounds,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            checkpoint_path=args.checkpoint,
            debug=args.debug,
            base_cr_path=args.base_cr,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
        )
    elif args.mode == "run-legacy":
        run_gsod_pipeline(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            context_file=args.context,
            num_fields=args.num_fields,
            k=args.k,
            max_retries=args.max_retries,
            max_combos=args.max_combos,
            project_path=args.project_path,
            instrument_dir=args.instrument_dir,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            checkpoint_path=args.checkpoint,
            debug=args.debug,
            base_cr_path=args.base_cr,
            include_source_code=args.include_source_code,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
        )
    elif args.mode == "report":
        ckpt = _load_checkpoint(args.checkpoint)
        branch_meta_index = _build_branch_index(args.instrument_info)
        generate_pipeline_report(
            ckpt=ckpt,
            branch_meta_index=branch_meta_index,
            instrument_info_path=args.instrument_info,
            output_path=args.output,
        )
    elif args.mode == "explore-all":
        run_explore_all(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            context_file=args.context,
            max_retries=args.max_retries,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            checkpoint_path=args.checkpoint,
            debug=args.debug,
            base_cr_path=args.base_cr,
            no_llm=args.no_llm,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
            project_path=getattr(args, "project_path", ""),
            instrument_dir=getattr(args, "instrument_dir", ""),
            db_dir=getattr(args, "db_dir", ""),
        )
    elif args.mode == "testplan":
        run_testplan(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            context_file=args.context,
            max_rounds=args.max_rounds,
            max_retries=args.max_retries,
            k=args.k,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            field_relations_path=args.field_relations,
            project_path=args.project_path,
            instrument_dir=args.instrument_dir,
            include_source_code=args.include_source_code,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            checkpoint_path=args.checkpoint,
            debug=args.debug,
            base_cr_path=args.base_cr,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
            db_dir=getattr(args, "db_dir", ""),
        )
    elif args.mode == "testplan-probe":
        run_testplan_probe(
            checkpoint_path=args.checkpoint,
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            testcase_id=args.testcase_id,
            target_key=args.target_key,
            context_file=args.context,
            field_relations_path=args.field_relations,
            project_path=args.project_path,
            instrument_dir=args.instrument_dir,
            include_source_code=args.include_source_code,
            max_retries=args.max_retries,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            base_cr_path=args.base_cr,
            no_llm=args.no_llm,
            debug=args.debug,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
        )
    elif args.mode == "testplan-report":
        ckpt = _load_checkpoint(args.checkpoint)
        _instr_path = getattr(args, "instrument_info", "") or ""
        _bmi = _build_branch_index(_instr_path) if _instr_path else None
        generate_testplan_report(
            ckpt=ckpt,
            output_path=args.output,
            branch_meta_index=_bmi,
        )
    elif args.mode == "explore-all-report":
        ckpt = _load_checkpoint(args.checkpoint)
        _instr_path = getattr(args, "instr", "") or ""
        _bmi = _build_branch_index(_instr_path) if _instr_path else None
        generate_exploration_report(
            ckpt=ckpt, output_path=args.output, branch_meta_index=_bmi
        )
    elif args.mode == "coverage-test":

        targets_spec: List[dict] = []
        for part in args.targets.split(","):
            part = part.strip()
            if ":" not in part:
                parser.error(f"无效 target 格式: {part!r}  (期望 '<bi>:<true|false>')")
            bi_str, tv_str = part.split(":", 1)
            try:
                bi_int = int(bi_str.strip())
            except ValueError:
                parser.error(f"branch_index 必须是整数: {bi_str!r}")
            tv_str = tv_str.strip().lower()
            if tv_str not in ("true", "false", "1", "0"):
                parser.error(f"target_value 必须是 true/false: {tv_str!r}")
            targets_spec.append(
                {"branch_index": bi_int, "target_value": tv_str in ("true", "1")}
            )
        run_coverage_test(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            checkpoint_path=args.checkpoint,
            targets_spec=targets_spec,
            workdir_base=args.workdir_base,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            max_retries=args.max_retries,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            base_cr_path=args.base_cr,
            project_path=args.project_path,
            instrument_dir=args.instrument_dir,
            include_source_code=args.include_source_code,
            debug=args.debug,
            context_file=args.context,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
        )
    elif args.mode == "fault":
        _fault_types = [
            t.strip() for t in (args.fault_types or "crash").split(",") if t.strip()
        ]
        run_fault(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            testplan_checkpoint_path=args.testplan_checkpoint,
            fault_types=_fault_types,
            fault_manager_url=args.fault_manager_url,
            context_file=args.context,
            max_rounds=args.max_rounds,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            checkpoint_path=args.checkpoint,
            debug=args.debug,
            base_cr_path=args.base_cr,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
            db_dir=getattr(args, "db_dir", ""),
        )
    elif args.mode == "targeted":
        run_targeted(
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            targeted_config_path=args.targeted_config,
            context_file=args.context,
            max_retries=args.max_retries,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            checkpoint_path=args.checkpoint,
            debug=args.debug,
            base_cr_path=args.base_cr,
            no_llm=args.no_llm,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
            project_path=getattr(args, "project_path", ""),
            instrument_dir=getattr(args, "instrument_dir", ""),
        )
    elif args.mode == "validate":
        run_validate(
            ea_checkpoint_path=args.ea_checkpoint,
            config_path=args.config,
            instrument_info_path=args.instrument_info,
            targeted_config_path=getattr(args, "targeted_config", ""),
            fields=getattr(args, "fields", []),
            context_file=args.context,
            wait_sec=args.wait_sec,
            collect_max_wait=args.collect_max_wait,
            workdir_base=args.workdir_base,
            keep_cluster=args.keep_cluster,
            reuse_cluster_name=args.reuse_cluster,
            debug=args.debug,
            operator_image=args.operator_image,
            cr_kind=args.cr_kind,
            instrument_prefix=args.instrument_prefix,
            dry_run=getattr(args, "dry_run", False),
            base_cr_path=getattr(args, "base_cr", ""),
            max_retries=getattr(args, "max_retries", 3),
        )
    elif args.mode == "preflight":
        import os as _os

        from llm.constraints import load_constraints, run_preflight

        _ctx = args.context
        if not _ctx or not _os.path.exists(_ctx):
            parser.error(f"context.json 不存在: {_ctx!r}")
        _profile_dir = _os.path.dirname(_os.path.abspath(_ctx))
        if not args.force:
            _existing = load_constraints(_profile_dir)
            if _existing is not None:
                n = len(_existing.get("constraints", []))
                logger.info(
                    f"[preflight] constraint.json 已存在 ({n} 条约束)，"
                    f"如需重新生成请使用 --force"
                )
            else:
                run_preflight(_ctx, _profile_dir)
        else:
            run_preflight(_ctx, _profile_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()