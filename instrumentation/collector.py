import os
import signal
import socket
import subprocess
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

import requests


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        return s.getsockname()[1]


@contextmanager
def port_forward_context_v2(
    cluster_name: str,
    namespace: str = "data-service",
    service_name: str = "data-collection-service-external",
    service_port: int = 80,
    local_port: Optional[int] = None,
):
    """针对 collector/main2.py 服务的端口转发上下文管理器。

    与 coverage.py 中的 port_forward_context 相同机制，可独立使用。
    """
    if local_port is None:
        local_port = _find_free_port()

    kubeconfig = os.path.join(os.path.expanduser("~"), ".kube", f"kind-{cluster_name}")
    cmd = [
        "kubectl",
        "--kubeconfig",
        kubeconfig,
        "port-forward",
        "-n",
        namespace,
        f"svc/{service_name}",
        f"{local_port}:{service_port}",
    ]

    print(f"[PortFwd-v2] Starting: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )


    deadline = time.time() + 10
    ready = False
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            if s.connect_ex(("localhost", local_port)) == 0:
                ready = True
                s.close()
                break
            s.close()
        except Exception:
            pass
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(
                f"Port-forward exited unexpectedly.\nstdout: {out}\nstderr: {err}"
            )
        time.sleep(0.5)

    if not ready:
        proc.terminate()
        raise TimeoutError(
            f"Port-forward did not become ready within 10s on :{local_port}"
        )

    print(f"[PortFwd-v2] Ready on localhost:{local_port}")
    try:
        yield local_port
    finally:
        print("[PortFwd-v2] Stopping...")
        if os.name == "nt":
            proc.terminate()
        else:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                proc.kill()
            else:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    proc.kill()
            proc.wait()
        print("[PortFwd-v2] Stopped")


def fetch_instrumentation_after_ts(
    cluster_name: str,
    ts: int,
    resource: str,
    timeout: int = 30,
    namespace: str = "data-service",
    service_name: str = "data-collection-service-external",
    service_port: int = 80,
    local_port: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """从收集器获取 start_ts >= ts 的最早一条插桩数据。

    对应 collector/main2.py 的 POST /fetch?ts=<ts>&resource=<resource> 接口。

    Args:
        cluster_name: Kind 集群名称
        ts: 时间戳（epoch 毫秒），返回 start_ts >= ts 的最早记录
        resource: 资源标识符，格式 "<namespace>/<name>"
        timeout: HTTP 请求超时（秒）

    Returns:
        InstrumentInfo 对应的 dict，结构为:
            {
                "start_ts": int,
                "end_ts": int,
                "resource": str,
                "traces": [
                    {
                        "branch_index": int,
                        "value": bool,
                        "type": str,
                        "expressions": {
                            "<expression_index>": {
                                "expression_index": int,
                                "value": str,
                                "type": str,
                                "hit_case_index": int,
                                "variables": {
                                    "<variable_index>": {
                                        "variable_index": int,
                                        "value": str,
                                        "type": str,
                                        "kind": str,
                                    }
                                }
                            }
                        }
                    },
                    ...
                ]
            }
        如果没有匹配记录则返回 None。

    Raises:
        RuntimeError: HTTP 请求失败
    """
    branches = []
    with port_forward_context_v2(
        cluster_name=cluster_name,
        namespace=namespace,
        service_name=service_name,
        service_port=service_port,
        local_port=local_port,
    ) as port:
        url = f"http://localhost:{port}/fetch"
        print(f"[coverage_v2] POST {url}?ts={ts}&resource={resource}")
        try:
            resp = requests.post(
                url, params={"ts": ts, "resource": resource}, timeout=timeout
            )
            resp.raise_for_status()
            body = resp.json()
            data = body.get("data")
            if data is None:
                print(f"[coverage_v2] No data found for ts>={ts}, resource={resource}")
                return None
            print(
                f"[coverage_v2] Got data: start_ts={data.get('start_ts')}, "
                f"resource={data.get('resource')}, "
                f"traces={len(data.get('traces', []))}"
            )
            for item in data.get("traces", []):
                branches.append(item["branch_index"])


            return data
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"[coverage_v2] fetch failed: {e}") from e


@contextmanager
def port_forward_fault_manager(
    cluster_name: str,
    namespace: str = "data-service",
    service_name: str = "fault-manager",
    service_port: int = 80,
    local_port: Optional[int] = None,
):
    """Port-forward the in-cluster fault-manager service to localhost.

    Yields the local port so callers can build ``http://localhost:<port>``.
    Mirrors port_forward_context_v2 but targets fault-manager.
    """
    if local_port is None:
        local_port = _find_free_port()

    kubeconfig = os.path.join(os.path.expanduser("~"), ".kube", f"kind-{cluster_name}")
    cmd = [
        "kubectl",
        "--kubeconfig",
        kubeconfig,
        "port-forward",
        "-n",
        namespace,
        f"svc/{service_name}",
        f"{local_port}:{service_port}",
    ]

    print(f"[FaultMgrFwd] Starting: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )

    deadline = time.time() + 10
    ready = False
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            if s.connect_ex(("localhost", local_port)) == 0:
                ready = True
                s.close()
                break
            s.close()
        except Exception:
            pass
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(
                f"[FaultMgrFwd] Port-forward exited unexpectedly.\nstdout: {out}\nstderr: {err}"
            )
        time.sleep(0.5)

    if not ready:
        proc.terminate()
        raise TimeoutError(
            f"[FaultMgrFwd] Did not become ready within 10s on :{local_port}"
        )

    print(f"[FaultMgrFwd] Ready on localhost:{local_port}")
    try:
        yield local_port
    finally:
        print("[FaultMgrFwd] Stopping...")
        if os.name == "nt":
            proc.terminate()
        else:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                proc.kill()
            else:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    proc.kill()
            proc.wait()
        print("[FaultMgrFwd] Stopped")


def fetch_instrumentation_after_ts_with_retry(
    cluster_name: str,
    ts: int,
    resource: str,
    max_wait_sec: int = 30,
    poll_interval_sec: float = 2.0,
    **kwargs,
) -> Optional[Dict[str, Any]]:
    """轮询等待，直到出现 start_ts >= ts 的数据或超时。

    Args:
        cluster_name: Kind 集群名称
        ts: 时间戳（epoch 毫秒）
        resource: 资源标识符，格式 "<namespace>/<name>"
        max_wait_sec: 最多等待秒数
        poll_interval_sec: 每次重试间隔
        **kwargs: 传递给 fetch_instrumentation_after_ts

    Returns:
        同 fetch_instrumentation_after_ts，超时返回 None
    """
    infinite = max_wait_sec <= 0
    deadline = None if infinite else time.time() + max_wait_sec
    attempt = 0
    while True:
        if not infinite and time.time() >= deadline:
            break
        attempt += 1
        try:
            data = fetch_instrumentation_after_ts(
                cluster_name, ts, resource=resource, **kwargs
            )
            if data is not None:
                print(
                    f"[coverage_v2] Found data after {attempt} attempt(s) "
                    f"(start_ts={data.get('start_ts')})"
                )
                return data
        except Exception as e:
            print(f"[coverage_v2] Attempt {attempt} error: {e}")
        if not infinite:
            remaining = deadline - time.time()
            sleep = min(poll_interval_sec, remaining)
        else:
            sleep = poll_interval_sec
        if sleep > 0:
            time.sleep(sleep)
    print(
        f"[coverage_v2] Timed out waiting for data after ts={ts}, resource={resource} ({max_wait_sec}s)"
    )
    return None