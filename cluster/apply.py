import copy
import logging
import os
import re
import tempfile
import time
from typing import Dict, List, Optional, Tuple

import yaml

from acto.kubectl_client import KubectlClient
from cluster.env import (
    ControllerPodMissingError,
    _extract_webhook_rejection_reason,
    _wait_for_webhook_ready,
    force_delete_controller_pod,
)
from instrumentation.collector import fetch_instrumentation_after_ts_with_retry

logger = logging.getLogger(__name__)


_SCHEDULING_FIELDS = (
    "nodeSelector",
    "nodeAffinityLabels",
    "tolerations",
    "nodeName",
    "topologySpreadConstraints",
    "affinity",
    "priorityClassName",
)


_PROBE_FIELDS = (
    "livenessProbe",
    "readinessProbe",
    "startupProbe",
)


_OPERATOR_ERROR_LEVELS = re.compile(r'"level"\s*:\s*"(error|warn|fatal)"', re.I)
_OPERATOR_ERROR_KEYWORDS = re.compile(
    r"\b(error|invalid|forbidden|fail(ed)?|panic|fatal|denied|rejected)\b", re.I
)


def get_operator_log_line_count(
    kubectl_client: KubectlClient,
    namespace: str,
    container_name: str = "",
) -> int:
    """Return the current number of log lines in the operator pod.

    Used to establish a *before* baseline so that after apply we can fetch
    only the newly-produced lines.  Returns 0 on any failure.
    """
    args = ["logs", "-n", namespace, "--tail=10000"]
    if container_name:
        pod_name = _find_operator_pod(kubectl_client, namespace, container_name)
        if pod_name:
            args += [pod_name]
        else:
            return 0
    else:
        args += ["--selector=control-plane=controller-manager"]
    result = kubectl_client.kubectl(args, capture_output=True, text=True)
    if result.returncode != 0:
        return 0
    return len((result.stdout or "").splitlines())


def fetch_operator_error_logs(
    kubectl_client: KubectlClient,
    namespace: str,
    container_name: str = "",
    skip_lines: int = 0,
    max_lines: int = 200,
) -> List[str]:
    """Fetch new operator log lines since *skip_lines* and return only
    ERROR/WARN entries (deduped, capped at *max_lines*).

    Args:
        skip_lines: number of lines already seen before the apply (from
            get_operator_log_line_count).  Lines beyond this offset are
            considered "new" after the apply.
        max_lines: cap on how many tail lines to fetch from kubectl.
    """
    args = ["logs", "-n", namespace, f"--tail={max_lines}"]
    if container_name:
        pod_name = _find_operator_pod(kubectl_client, namespace, container_name)
        if pod_name:
            args += [pod_name]
        else:
            return []
    else:
        args += ["--selector=control-plane=controller-manager"]
    result = kubectl_client.kubectl(args, capture_output=True, text=True)
    if result.returncode != 0:
        return []
    lines = (result.stdout or "").splitlines()

    if skip_lines > 0 and len(lines) > skip_lines:
        lines = lines[skip_lines:]

    errors: List[str] = []
    seen: set = set()
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if _OPERATOR_ERROR_LEVELS.search(stripped) or _OPERATOR_ERROR_KEYWORDS.search(
            stripped
        ):

            key = re.sub(r"[\d\-T:\.Z]{8,}|[0-9a-f\-]{32,}", "", stripped)[:200]
            if key not in seen:
                seen.add(key)
                errors.append(stripped[:500])
    return errors


def _find_operator_pod(
    kubectl_client: KubectlClient,
    namespace: str,
    container_name: str,
) -> str:
    """Return the name of the first running pod whose containers include
    *container_name*.  Returns empty string on failure."""
    result = kubectl_client.kubectl(
        [
            "get",
            "pods",
            "-n",
            namespace,
            "--no-headers",
            "-o",
            "custom-columns=NAME:.metadata.name,PHASE:.status.phase",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ""
    for line in (result.stdout or "").splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].lower() == "running":
            pod = parts[0]

            ci = kubectl_client.kubectl(
                [
                    "get",
                    "pod",
                    pod,
                    "-n",
                    namespace,
                    "-o",
                    "jsonpath={.spec.containers[*].name}",
                ],
                capture_output=True,
                text=True,
            )
            if container_name in (ci.stdout or ""):
                return pod
    return ""


def _strip_scheduling_fields(cr: dict) -> dict:
    """Return a deep copy of *cr* with scheduling-related spec fields removed.

    Removes fields listed in _SCHEDULING_FIELDS from .spec (top-level only,
    which covers CassandraDatacenter and most other operators).
    Logs each removed field at INFO level.
    """
    cr = copy.deepcopy(cr)
    spec = cr.get("spec")
    if not isinstance(spec, dict):
        return cr
    removed = []
    for field in _SCHEDULING_FIELDS:
        if field in spec:
            del spec[field]
            removed.append(field)
    if removed:
        logger.info("[strip-scheduling] 已删除调度约束字段: %s", ", ".join(removed))
    return cr


def _strip_probe_fields(cr: dict) -> dict:
    """Return a deep copy of *cr* with liveness/readiness/startup probe fields removed.

    Removes probe fields from .spec (top-level only).  Most operators surface
    probes at spec level (e.g. RabbitMQ, CassOp) and invalid probe configs
    cause controller reconciliation errors.
    Logs each removed field at INFO level.
    """
    cr = copy.deepcopy(cr)
    spec = cr.get("spec")
    if not isinstance(spec, dict):
        return cr
    removed = []
    for field in _PROBE_FIELDS:
        if field in spec:
            del spec[field]
            removed.append(field)
    if removed:
        logger.info("[strip-probes] 已删除探针字段: %s", ", ".join(removed))
    return cr


_last_create_stderr: str = ""


def apply_cr_and_collect(
    kubectl_client: KubectlClient,
    namespace: str,
    cluster_name: str,
    input_cr: dict,
    operator_container_name: str = "",
    wait_sec: int = 6,
    collect_max_wait: int = 30,
    skip_cluster_reset: bool = False,
    operator_namespace: str = "",
    instrument_prefix: str = "",
    strip_scheduling: bool = True,
    strip_probes: bool = True,
) -> Tuple[Optional[Dict], int, bool, bool]:
    """Delete existing CR → delete controller pod → create CR fresh → collect data.

    每次执行完整的删除-重建周期，确保 CR status 不会污染下一轮的收集数据。

    Args:
        operator_namespace: namespace where the operator Pod lives (may differ from
            the CR namespace). Defaults to ``namespace`` if not provided.
        instrument_prefix: 用于 data-service 查询的 resource 前缀（operator 插桦时自定义，
            如 CassandraDatacenterReconciler）。为空时回退到 CR 的 kind 字段。
        skip_cluster_reset: 如果为 True，跳过删除 CR / 重启 Pod / 等待 webhook 步骤，
            直接尝试 kubectl create。适用于上一次尝试被 webhook 拒绝且 CR 未写入集群时。

    流程:
      1. 删除已有 CR（忽略不存在的错误）
      2. 强制删除控制器 Pod 并等待其重新就绪
      3. 等待 webhook 可用
      4. kubectl create 新 CR（带重试）
      5. 等待 wait_sec 后从收集器拉取插桩数据

    Returns:
        (instr_data, ts_ms, create_ok, is_webhook_rejection, cluster_dead)
        is_webhook_rejection=True 表示 CR 被 webhook 拒绝（无副作用，下次可跳过集群重置）
        cluster_dead=True 表示控制器 Pod 无法恢复，需要重建集群
    """
    if strip_scheduling:
        input_cr = _strip_scheduling_fields(input_cr)
    if strip_probes:
        input_cr = _strip_probe_fields(input_cr)
    meta = input_cr.get("metadata", {})
    cr_ns = meta.get("namespace", namespace)
    op_ns = operator_namespace or namespace
    cr_name = meta.get("name", "")
    cr_kind = input_cr.get("kind", "")

    if skip_cluster_reset:
        logger.info("[1-4/5] 跳过集群重置（上次为 webhook 拒绝，无副作用）")
    else:


        if cr_kind and cr_name:
            logger.info(f"[1/4] 删除已有 CR: {cr_kind}/{cr_name} (ns={cr_ns})")
            del_result = kubectl_client.kubectl(
                ["delete", cr_kind, cr_name, "-n", cr_ns, "--ignore-not-found"],
                capture_output=True,
                text=True,
            )
            if del_result.returncode != 0:
                logger.warning(f"删除 CR 返回非零: {del_result.stderr[:200]}")
            else:
                stdout = (del_result.stdout or "").strip()
                if stdout and "deleted" in stdout.lower():
                    logger.info(f"CR {cr_name} 已触发删除，等待 finalizer 完成...")
                else:
                    logger.info(f"CR {cr_name} 不存在，无需删除")
        else:
            logger.warning("[1/4] CR kind/name 缺失，跳过删除步骤")


        if cr_kind and cr_name:
            logger.info(f"[2/4] 等待 CR {cr_name} 完全消失（finalizer 清理）...")
            _t2 = time.monotonic()
            for i in range(60):
                time.sleep(3)
                check = kubectl_client.kubectl(
                    [
                        "get",
                        cr_kind,
                        cr_name,
                        "-n",
                        cr_ns,
                        "--ignore-not-found",
                        "--no-headers",
                        "-o",
                        "name",
                    ],
                    capture_output=True,
                    text=True,
                )
                if check.returncode == 0 and not (check.stdout or "").strip():
                    logger.info(
                        f"⏱  [CR 删除等待] 完成  {(i + 1) * 3}s  ({time.monotonic() - _t2:.1f}s total)"
                    )
                    break
            else:
                logger.warning("等待 CR 消失超时 (180s)，继续执行")


        logger.info("[3/5] 强制重启控制器 Pod...")
        _t3 = time.monotonic()
        try:
            force_delete_controller_pod(
                kubectl_client, op_ns, operator_container_name or None
            )
        except ControllerPodMissingError as _e:
            logger.error(f"控制器 Pod 无法恢复，需要重建集群: {_e}")
            return None, 0, False, False, True
        logger.info(f"⏱  [Pod 重启等待] 完成  {time.monotonic() - _t3:.1f}s")


        logger.info("[4/5] 等待 webhook 就绪...")
        _t4 = time.monotonic()
        _wait_for_webhook_ready(kubectl_client, op_ns, timeout=60)
        logger.info(f"⏱  [Webhook 就绪等待] 完成  {time.monotonic() - _t4:.1f}s")


    cr_yaml_str = yaml.dump(input_cr, default_flow_style=False)
    logger.info(f"[5/5] 创建新 CR: {cr_kind}/{cr_name}")
    logger.debug(f"CR YAML:\n{cr_yaml_str}")
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as f:
        f.write(cr_yaml_str)
        cr_file = f.name

    ts_ms = int(time.time() * 1000)
    create_ok = False
    last_stderr = ""
    is_webhook_rejection = False
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        result = kubectl_client.kubectl(
            ["create", "-f", cr_file, "-n", cr_ns],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            create_ok = True
            ts_ms = int(time.time() * 1000)
            break
        stderr = result.stderr or ""
        last_stderr = stderr
        lower = stderr.lower()
        is_transient = (
            "connection refused" in lower
            or "no endpoints available" in lower
            or "context deadline exceeded" in lower
            or "timeout" in lower
            or "etcdserver" in lower
        )
        is_crd_missing = (
            "no matches for kind" in lower
            or "ensure crds are installed" in lower
            or "resource mapping not found" in lower
        )
        is_rejected = (
            not is_transient
            and not is_crd_missing
            and (
                "forbidden" in lower
                or "denied the request" in lower
                or "admission webhook" in lower
                or " invalid:" in lower
                or "error validating" in lower
                or "unknown field" in lower
                or "is invalid" in lower
            )
        )
        if is_crd_missing:
            logger.error(f"CRD 未安装，集群状态异常，需要重建: {stderr[:400]}")
            try:
                os.unlink(cr_file)
            except Exception:
                pass
            return None, 0, False, False, True
        if is_rejected:
            is_webhook_rejection = True
            reason = _extract_webhook_rejection_reason(stderr)
            logger.warning(
                f"kubectl create 被拒绝（不可重试）: {reason[:400]}\n"
                f"  full stderr: {stderr[:600]}"
            )
            print(f"Error: {reason[:400]}\nstderr: {stderr[:600]}")
            break
        wait = attempt * 3
        logger.warning(
            f"kubectl create 重试 {attempt}/{max_retries}，等待 {wait}s\n"
            f"  stderr: {stderr[:600]}"
        )
        time.sleep(wait)

    try:
        os.unlink(cr_file)
    except Exception:
        pass

    if not create_ok:
        logger.error(f"kubectl create 最终失败: {last_stderr[:300]}")
        global _last_create_stderr
        _last_create_stderr = last_stderr
        return None, ts_ms, False, is_webhook_rejection, False

    logger.info(f"CR create 成功，ts={ts_ms}，等待 {wait_sec}s 让控制器处理...")
    time.sleep(wait_sec)
    logger.info(f"⏱  [控制器稳定等待] 完成  {wait_sec}s")


    is_webhook_rejection = False
    resource_id = (
        f"{instrument_prefix}/{cr_ns}/{cr_name}"
        if instrument_prefix
        else f"{cr_ns}/{cr_name}"
    )


    _t_collect = time.monotonic()
    instr_data = fetch_instrumentation_after_ts_with_retry(
        cluster_name=cluster_name,
        ts=ts_ms,
        resource=resource_id,
        max_wait_sec=collect_max_wait,
        poll_interval_sec=3.0,
    )
    logger.info(f"⏱  [插桩数据收集] 完成  {time.monotonic() - _t_collect:.1f}s")
    return instr_data, ts_ms, True, False, False