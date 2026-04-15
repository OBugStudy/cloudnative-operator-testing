import importlib
import json
import json as _json
import logging
import os
import time
from typing import Dict, List, Optional

import yaml

from acto.common import kubernetes_client
from acto.deploy import Deploy
from acto.kubectl_client import KubectlClient
from acto.kubernetes_engine import kind
from acto.lib.operator_config import OperatorConfig
from acto.runner import Runner
from acto.utils.image_helper import ImageHelper
from acto.utils.preprocess import process_crd

logger = logging.getLogger(__name__)


class ControllerPodMissingError(RuntimeError):
    """控制器 Pod 消失且无法在预期时间内恢复，需要重建集群。"""


def _setup_cluster_env(
    config: OperatorConfig,
    config_dir: str,
    gsod_context: dict,
    workdir: str,
    cluster_name: str,
    operator_image: str = "",
) -> dict:
    """创建 Kind 集群、部署 Operator、创建 Runner

    Returns:
        dict with keys: cluster, cluster_name, context_name, kubeconfig,
                        kubectl_client, namespace, operator_container_name,
                        runner, seed_cr, deploy
        若失败返回 None
    """
    cluster = kind.Kind(
        acto_namespace=0,
        feature_gates=config.kubernetes_engine.feature_gates,
        num_nodes=config.num_nodes,
        version=config.kubernetes_version,
    )
    context_name = cluster.get_context_name(cluster_name)
    kubeconfig = os.path.join(os.path.expanduser("~"), ".kube", context_name)

    image_archive = ImageHelper.prepare_image_archive(
        gsod_context.get("preload_images", set())
    )
    cluster.restart_cluster(cluster_name, kubeconfig, context_name)
    cluster.load_images(image_archive, cluster_name)
    cluster.load_images_from_docker(
        ["data-collection-service:latest"], cluster_name, operator_image
    )


    deploy = Deploy(config.deploy)
    kubectl_client = KubectlClient(kubeconfig, context_name)
    if not deploy.deploy_with_retry(
        kubeconfig, context_name, kubectl_client, "deploy-ns"
    ):
        logger.error(f"[{cluster_name}] 部署 Operator 失败")
        cluster.delete_cluster(cluster_name, kubeconfig)
        return None


    with open(config.seed_custom_resource, "r", encoding="utf-8") as f:
        seed_cr = yaml.safe_load(f.read())
        seed_cr["metadata"]["name"] = "test-cluster"

    namespace = deploy.operator_existing_namespace or "deploy-ns"
    operator_container_name = deploy.operator_container_name


    crd = process_crd(
        kubernetes_client(kubeconfig, context_name),
        KubectlClient(kubeconfig, context_name),
        config.crd_name,
        config.crd_version,
    )
    run_context = {"namespace": namespace, "crd": crd, "preload_images": set()}

    custom_runner_hooks = None
    if config.custom_runner:
        module = importlib.import_module(config.custom_runner)
        custom_runner_hooks = getattr(module, "CUSTOM_RUNNER_HOOKS", None)

    runner = Runner(
        run_context,
        workdir,
        kubeconfig,
        context_name,
        operator_container_name=operator_container_name,
        custom_runner_hooks=custom_runner_hooks,
    )

    return {
        "cluster": cluster,
        "cluster_name": cluster_name,
        "context_name": context_name,
        "kubeconfig": kubeconfig,
        "kubectl_client": kubectl_client,
        "namespace": namespace,
        "operator_container_name": operator_container_name,
        "runner": runner,
        "seed_cr": seed_cr,
        "deploy": deploy,
    }


def _wait_for_controller_pod_ready(
    kubectl_client: KubectlClient,
    namespace: str,
    operator_container_name: str = None,
    timeout_sec: int = 120,
) -> bool:
    """轮询直到指定 namespace 中的控制器 Pod 进入 Running+Ready 状态。

    Returns:
        True 如果 Pod 就绪，False 如果超时。
    """
    interval = 2
    steps = timeout_sec // interval
    for i in range(steps):
        time.sleep(interval)
        result = kubectl_client.kubectl(
            ["get", "pods", "-n", namespace, "-o", "json"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        pods = json.loads(result.stdout)
        for pod in pods.get("items", []):
            if operator_container_name:
                containers = pod.get("spec", {}).get("containers", [])
                if not any(
                    c.get("name") == operator_container_name for c in containers
                ):
                    continue
            phase = pod.get("status", {}).get("phase", "")
            conditions = pod.get("status", {}).get("conditions", [])
            ready = any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            )
            if phase == "Running" and ready:
                logger.info(f"控制器 Pod 已就绪 ({(i + 1) * interval}s)")
                return True
    return False


def _namespace_has_deployment(kubectl_client: KubectlClient, namespace: str) -> bool:
    """检查指定 namespace 中是否存在至少一个 Deployment。"""
    result = kubectl_client.kubectl(
        ["get", "deployments", "-n", namespace, "--no-headers"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and bool((result.stdout or "").strip())


def force_delete_controller_pod(
    kubectl_client: KubectlClient, namespace: str, operator_container_name: str = None
) -> bool:
    """强制删除控制器 Pod 并等待新 Pod 就绪。

    异常情况处理：若当前 namespace 中找不到控制器 Pod，但存在 Deployment（说明
    控制器曾被强制删除后尚未重新调度），则直接等待 Deployment 拉起新 Pod，而非
    静默跳过。这可以防止因控制器 Pod 缺失导致后续 CR apply 无法产生插桩数据。

    Returns:
        True 如果控制器 Pod 就绪，False 如果超时。
    """
    result = kubectl_client.kubectl(
        ["get", "pods", "-n", namespace, "-o", "json"], capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"获取 Pod 列表失败: {result.stderr}")
        return False

    pods = json.loads(result.stdout)
    controller_pod = None
    for pod in pods.get("items", []):
        containers = pod.get("spec", {}).get("containers", [])
        for c in containers:
            if operator_container_name and c.get("name") == operator_container_name:
                controller_pod = pod["metadata"]["name"]
                break
            elif not operator_container_name:
                ns = pod["metadata"].get("namespace", "")
                if ns != "kube-system":
                    controller_pod = pod["metadata"]["name"]
                    break
        if controller_pod:
            break

    if not controller_pod:

        if _namespace_has_deployment(kubectl_client, namespace):
            logger.warning(
                "未找到控制器 Pod，但 namespace 中存在 Deployment——"
                "先等待 30s 看是否自行恢复..."
            )
            ok = _wait_for_controller_pod_ready(
                kubectl_client, namespace, operator_container_name, timeout_sec=30
            )
            if ok:
                return True

            raise ControllerPodMissingError(
                f"控制器 Pod 在 namespace '{namespace}' 中消失且 30s 内未恢复，"
                "需要重建集群"
            )
        else:
            raise ControllerPodMissingError(
                f"namespace '{namespace}' 中既无控制器 Pod 也无 Deployment——"
                "Operator 可能已完全消失，需要重建集群"
            )

    logger.info(f"强制删除控制器 Pod: {controller_pod}")
    del_result = kubectl_client.kubectl(
        [
            "delete",
            "pod",
            controller_pod,
            "-n",
            namespace,
            "--force",
            "--grace-period=0",
        ],
        capture_output=True,
        text=True,
    )
    if del_result.returncode != 0:
        logger.warning(f"删除 Pod 返回非零: {del_result.stderr}")

    logger.info("等待控制器 Pod 重启...")
    ok = _wait_for_controller_pod_ready(
        kubectl_client, namespace, operator_container_name, timeout_sec=120
    )
    if not ok:
        logger.error("等待控制器 Pod 就绪超时 (120s)")
    return ok


def _wait_for_webhook_ready(
    kubectl_client: KubectlClient, namespace: str, timeout: int = 30
) -> bool:
    """等待 namespace 中 webhook 相关 endpoints 就绪

    通过检查 ValidatingWebhookConfiguration / MutatingWebhookConfiguration
    关联的 Service 是否有 Ready 的 Endpoints 来判断 webhook 是否真正可用。
    """

    webhook_services = set()
    for wh_type in ["validatingwebhookconfigurations", "mutatingwebhookconfigurations"]:
        r = kubectl_client.kubectl(
            ["get", wh_type, "-o", "json"],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            continue
        try:
            items = json.loads(r.stdout).get("items", [])
            for wh in items:
                for hook in wh.get("webhooks", []):
                    svc = hook.get("clientConfig", {}).get("service", {})
                    svc_ns = svc.get("namespace", "")
                    svc_name = svc.get("name", "")
                    if svc_ns == namespace and svc_name:
                        webhook_services.add(svc_name)
        except (json.JSONDecodeError, KeyError):
            continue

    if not webhook_services:
        logger.debug("[webhook] 未发现与 namespace 关联的 webhook service，跳过等待")
        return True

    logger.info(
        f"[webhook] 等待 webhook service 就绪: {', '.join(webhook_services)} "
        f"(超时 {timeout}s)"
    )

    deadline = time.time() + timeout
    while time.time() < deadline:
        all_ready = True
        for svc_name in webhook_services:
            r = kubectl_client.kubectl(
                ["get", "endpoints", svc_name, "-n", namespace, "-o", "json"],
                capture_output=True,
                text=True,
            )
            if r.returncode != 0:
                all_ready = False
                break
            try:
                ep = json.loads(r.stdout)
                subsets = ep.get("subsets", [])
                has_addr = any(len(s.get("addresses", [])) > 0 for s in subsets)
                if not has_addr:
                    all_ready = False
                    break
            except (json.JSONDecodeError, KeyError):
                all_ready = False
                break

        if all_ready:
            logger.info("[webhook] 所有 webhook endpoints 就绪")

            time.sleep(2)
            return True
        time.sleep(2)

    logger.warning(f"[webhook] 等待超时 ({timeout}s)，将尝试 apply")
    return False


def _dump_webhook_diagnostics(kubectl_client: KubectlClient, namespace: str):
    """kubectl apply 最终失败时，输出诊断信息帮助排查 webhook 问题"""
    logger.error("[诊断] === Webhook 失败诊断信息 ===")


    r = kubectl_client.kubectl(
        ["get", "pods", "-n", namespace, "-o", "wide"],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        logger.error(f"[诊断] Pod 状态:\n{r.stdout.strip()}")
    else:
        logger.error(f"[诊断] 获取 Pod 失败: {r.stderr}")


    r = kubectl_client.kubectl(
        ["get", "endpoints", "-n", namespace, "-o", "wide"],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        logger.error(f"[诊断] Endpoints:\n{r.stdout.strip()}")


    r = kubectl_client.kubectl(
        ["get", "svc", "-n", namespace, "-o", "wide"],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        logger.error(f"[诊断] Services:\n{r.stdout.strip()}")


    for wh_type in ["validatingwebhookconfigurations", "mutatingwebhookconfigurations"]:
        r = kubectl_client.kubectl(
            ["get", wh_type, "-o", "json"],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            continue
        try:
            items = json.loads(r.stdout).get("items", [])
            for wh in items:
                name = wh.get("metadata", {}).get("name", "?")
                for hook in wh.get("webhooks", []):
                    hook_name = hook.get("name", "?")
                    svc = hook.get("clientConfig", {}).get("service", {})
                    svc_ns = svc.get("namespace", "")
                    svc_name = svc.get("name", "")
                    svc_port = svc.get("port", "?")
                    failure_policy = hook.get("failurePolicy", "?")
                    if svc_ns == namespace:
                        logger.error(
                            f"[诊断] {wh_type}: {name}/{hook_name} "
                            f"→ {svc_name}:{svc_port} (failurePolicy={failure_policy})"
                        )
        except (json.JSONDecodeError, KeyError):
            pass


    r = kubectl_client.kubectl(
        ["get", "pods", "-n", namespace, "-o", "json"],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        try:
            pods = json.loads(r.stdout).get("items", [])
            for pod in pods:
                pod_name = pod.get("metadata", {}).get("name", "")
                phase = pod.get("status", {}).get("phase", "")
                if phase == "Running":
                    log_r = kubectl_client.kubectl(
                        ["logs", pod_name, "-n", namespace, "--tail=20"],
                        capture_output=True,
                        text=True,
                    )
                    if log_r.returncode == 0 and log_r.stdout.strip():
                        logger.error(
                            f"[诊断] Pod {pod_name} 最后 20 行日志:\n"
                            f"{log_r.stdout.strip()}"
                        )
        except (json.JSONDecodeError, KeyError):
            pass

    logger.error("[诊断] === 诊断信息结束 ===")


def _extract_webhook_rejection_reason(stderr: str) -> str:
    """从 kubectl apply 的 stderr 中提取 webhook 拒绝原因

    webhook 拒绝的 stderr 格式通常为:
      Error from server (Forbidden): error when applying patch: ... : admission webhook "..." denied the request: <原因>
    或:
      Error from server (Forbidden): error when creating: ... : admission webhook "..." denied the request: <原因>
    """

    marker = "denied the request:"
    idx = stderr.find(marker)
    if idx >= 0:
        reason = stderr[idx + len(marker) :].strip()
        if reason:
            return reason[:500]


    marker2 = "(Forbidden):"
    idx2 = stderr.find(marker2)
    if idx2 >= 0:
        rest = stderr[idx2 + len(marker2) :].strip()

        for skip in ["error when applying patch:", "error when creating:"]:
            if rest.startswith(skip):
                rest = rest[len(skip) :].strip()


        lines = rest.strip().split("\n")
        if len(lines) > 1:
            return lines[-1].strip()[:500]

        if len(rest) > 300:
            return "..." + rest[-300:]
        return rest[:500]


    return stderr.strip()[-300:] if len(stderr) > 300 else stderr.strip()


def _attach_cluster_env(
    config: "OperatorConfig",
    config_dir: str,
    cluster_name: str,
    workdir: str,
) -> Optional[Dict]:
    """连接到一个已有的 Kind 集群，跳过创建和部署步骤。

    仅重建 KubectlClient 并加载种子 CR，其余字段与 _setup_cluster_env 返回结构保持一致。
    若集群不存在则返回 None。
    """

    from acto.deploy import Deploy
    from acto.kubectl_client import KubectlClient
    from acto.kubernetes_engine import kind

    cluster = kind.Kind(
        acto_namespace=0,
        feature_gates=config.kubernetes_engine.feature_gates,
        num_nodes=config.num_nodes,
        version=config.kubernetes_version,
    )
    context_name = cluster.get_context_name(cluster_name)
    kubeconfig = os.path.join(os.path.expanduser("~"), ".kube", context_name)

    if not os.path.exists(kubeconfig):
        logger.error(f"[attach] kubeconfig 不存在: {kubeconfig}，集群可能未创建")
        return None

    kubectl_client = KubectlClient(kubeconfig, context_name)


    probe = kubectl_client.kubectl(
        ["get", "nodes", "--no-headers"],
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        logger.error(f"[attach] 集群 {cluster_name} 不可达: {probe.stderr[:200]}")
        return None

    deploy = Deploy(config.deploy)
    namespace = deploy.operator_existing_namespace or "deploy-ns"
    operator_container_name = deploy.operator_container_name

    with open(config.seed_custom_resource, "r", encoding="utf-8") as f:
        seed_cr = yaml.safe_load(f.read())
        seed_cr["metadata"]["name"] = "test-cluster"

    logger.info(f"[attach] 已连接到集群 {cluster_name}，namespace={namespace}")
    return {
        "cluster": cluster,
        "cluster_name": cluster_name,
        "context_name": context_name,
        "kubeconfig": kubeconfig,
        "kubectl_client": kubectl_client,
        "namespace": namespace,
        "operator_container_name": operator_container_name,
        "runner": None,
        "seed_cr": seed_cr,
        "deploy": deploy,
    }


def _is_pod_unschedulable(pod: dict) -> str:
    """Return a reason string if the pod is permanently unschedulable, else ''."""
    phase = (pod.get("status") or {}).get("phase", "")
    if phase not in ("Pending", ""):
        return ""
    for cond in (pod.get("status") or {}).get("conditions", []):
        if cond.get("type") == "PodScheduled" and cond.get("status") == "False":
            if cond.get("reason", "") == "Unschedulable":
                name = pod.get("metadata", {}).get("name", "?")
                msg = cond.get("message", "")
                return f"Pod {name} Unschedulable: {msg[:200]}"
    return ""


def _wait_for_steady_state(
    kubectl_client: KubectlClient,
    namespace: str,
    max_wait_sec: int = 480,
    poll_interval: int = 5,
    extra_namespaces: Optional[List[str]] = None,
) -> bool:
    """Poll until all StatefulSets and Deployments in *namespace* (and any
    *extra_namespaces*) have readyReplicas == spec.replicas (or desired == 0).

    Bails out immediately if any pod is permanently Unschedulable (node
    selector / taint mismatch), since those pods will never become ready.

    Returns True if steady state reached, False on timeout or unschedulable pod.
    """
    namespaces = list(dict.fromkeys([namespace] + (extra_namespaces or [])))
    deadline = time.monotonic() + max_wait_sec
    last_log_t = time.monotonic() - 999
    log_interval = 15

    logger.info(
        "[steady] 等待集群稳态 (最长 %ds, ns=%s)...",
        max_wait_sec,
        ", ".join(namespaces),
    )

    while time.monotonic() < deadline:
        all_ready = True
        not_ready_summary: List[str] = []
        try:
            for ns in namespaces:

                rp = kubectl_client.kubectl(
                    ["get", "pods", "-n", ns, "-o", "json"],
                    capture_output=True,
                    text=True,
                )
                if rp.returncode == 0 and rp.stdout:
                    for pod in _json.loads(rp.stdout).get("items", []):
                        reason = _is_pod_unschedulable(pod)
                        if reason:
                            logger.warning(
                                "\033[33m[steady] %s — 永久无法调度，跳过稳态等待\033[0m",
                                reason,
                            )
                            return False

                for resource in ("statefulsets", "deployments"):
                    r = kubectl_client.kubectl(
                        ["get", resource, "-n", ns, "-o", "json"],
                        capture_output=True,
                        text=True,
                    )
                    if r.returncode != 0 or not r.stdout:
                        all_ready = False
                        not_ready_summary.append(f"{resource}(ns={ns}) 查询失败")
                        break
                    items = _json.loads(r.stdout).get("items", [])
                    for obj in items:
                        desired = (obj.get("spec") or {}).get("replicas", 0)
                        if desired == 0:
                            continue
                        ready = (obj.get("status") or {}).get("readyReplicas", 0) or 0
                        if ready < desired:
                            name = obj.get("metadata", {}).get("name", "?")
                            not_ready_summary.append(
                                f"{resource}/{name}(ns={ns}) {ready}/{desired}"
                            )
                            all_ready = False
                    if not all_ready:
                        break
                if not all_ready:
                    break
        except Exception as exc:
            logger.debug("[steady] 稳态轮询异常: %s", exc)
            all_ready = False

        now = time.monotonic()
        elapsed = now - (deadline - max_wait_sec)

        if all_ready:
            logger.info("\033[32m[steady] 集群已达稳态 (%.1fs)\033[0m", elapsed)
            return True

        if now - last_log_t >= log_interval:
            logger.info(
                "\033[36m[steady] 等待中 %.0f/%.0fs — %s\033[0m",
                elapsed,
                max_wait_sec,
                "; ".join(not_ready_summary) or "...",
            )
            last_log_t = now

        time.sleep(poll_interval)

    logger.warning("\033[33m[steady] 等待稳态超时 (%ds)\033[0m", max_wait_sec)
    return False


def _check_cluster_health(
    kubectl_client: KubectlClient,
    namespace: str,
    *,
    extra_namespaces: Optional[list] = None,
) -> Optional[str]:
    """Check cluster health after a CR apply and return a bug description or None.

    Checks (in order):
      1. Pods in CrashLoopBackOff or Error state.
      2. StatefulSets where spec.replicas != status.readyReplicas.
      3. Deployments where spec.replicas != status.readyReplicas or not Available.

    Returns:
        None  — cluster is healthy.
        str   — human-readable description of the detected problem (bug).
    """
    namespaces = [namespace] + (extra_namespaces or [])
    issues = []

    for ns in namespaces:

        try:
            r = kubectl_client.kubectl(
                ["get", "pods", "-n", ns, "-o", "json"],
                capture_output=True,
                text=True,
            )
            if r.returncode == 0 and r.stdout:
                pod_list = _json.loads(r.stdout).get("items", [])
                for pod in pod_list:
                    name = pod.get("metadata", {}).get("name", "?")
                    phase = pod.get("status", {}).get("phase", "")

                    unschedulable = _is_pod_unschedulable(pod)
                    if unschedulable:
                        issues.append(unschedulable)
                        continue
                    if phase in ("Failed",):
                        issues.append(f"Pod {name} (ns={ns}) phase={phase}")
                        continue
                    for cs in pod.get("status", {}).get("containerStatuses", []):
                        cname = cs.get("name", "?")
                        state = cs.get("state", {})
                        waiting_reason = (state.get("waiting") or {}).get("reason", "")
                        terminated_reason = (state.get("terminated") or {}).get(
                            "reason", ""
                        )
                        restart_count = cs.get("restartCount", 0)
                        if waiting_reason in ("CrashLoopBackOff", "OOMKilled", "Error"):
                            issues.append(
                                f"Pod {name}/{cname} (ns={ns}) waiting={waiting_reason}"
                            )
                        elif terminated_reason == "Error":
                            issues.append(
                                f"Pod {name}/{cname} (ns={ns}) terminated=Error"
                            )
                        elif restart_count > 0 and phase != "Running":
                            issues.append(
                                f"Pod {name}/{cname} (ns={ns}) restartCount={restart_count} phase={phase}"
                            )
        except Exception as _pe:
            logger.debug("[health] pod 检查异常: %s", _pe)


        try:
            r = kubectl_client.kubectl(
                ["get", "statefulsets", "-n", ns, "-o", "json"],
                capture_output=True,
                text=True,
            )
            if r.returncode == 0 and r.stdout:
                ss_list = _json.loads(r.stdout).get("items", [])
                for ss in ss_list:
                    sname = ss.get("metadata", {}).get("name", "?")
                    desired = (ss.get("spec") or {}).get("replicas", 0)
                    ready = (ss.get("status") or {}).get("readyReplicas", 0) or 0
                    if desired > 0 and ready < desired:
                        issues.append(
                            f"StatefulSet {sname} (ns={ns}) desired={desired} ready={ready}"
                        )
        except Exception as _se:
            logger.debug("[health] statefulset 检查异常: %s", _se)


        try:
            r = kubectl_client.kubectl(
                ["get", "deployments", "-n", ns, "-o", "json"],
                capture_output=True,
                text=True,
            )
            if r.returncode == 0 and r.stdout:
                dep_list = _json.loads(r.stdout).get("items", [])
                for dep in dep_list:
                    dname = dep.get("metadata", {}).get("name", "?")
                    desired = (dep.get("spec") or {}).get("replicas", 0)
                    ready = (dep.get("status") or {}).get("readyReplicas", 0) or 0
                    if desired > 0 and ready < desired:
                        issues.append(
                            f"Deployment {dname} (ns={ns}) desired={desired} ready={ready}"
                        )
                    for cond in (dep.get("status") or {}).get("conditions", []):
                        if (
                            cond.get("type") == "Available"
                            and cond.get("status") != "True"
                        ):
                            issues.append(
                                f"Deployment {dname} (ns={ns}) condition=Available "
                                f"status={cond.get('status')} msg={cond.get('message', '')[:120]}"
                            )
        except Exception as _de:
            logger.debug("[health] deployment 检查异常: %s", _de)

    if issues:
        return "; ".join(issues)
    return None