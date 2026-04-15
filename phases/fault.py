

import json
import logging
import random as _random
import time
from typing import Dict, List, Optional

import requests
import yaml

from acto.kubectl_client import KubectlClient
from checkpoint.store import _save_checkpoint
from cluster.apply import apply_cr_and_collect
from cluster.env import _check_cluster_health, _wait_for_steady_state
from core.rich_logger import update_progress, update_status
from instrumentation.diff import diff_branch_sequences
from relations.tracker import _update_field_relations_from_diff

logger = logging.getLogger(__name__)


HEALTH_POLL_INTERVAL = 5
HEALTH_MAX_WAIT = 120
STUCK_THRESHOLD = 5


def _inject_crash(
    kubectl_client: KubectlClient,
    namespace: str,
    operator_container_name: str,
) -> bool:
    """Delete the operator Pod so the ReplicaSet restarts it immediately.

    Returns True if a pod was found and deleted, False otherwise.
    """
    try:
        result = kubectl_client.kubectl(
            [
                "get",
                "pods",
                "-n",
                namespace,
                "--no-headers",
                "-o",
                "custom-columns=NAME:.metadata.name",
            ],
            capture_output=True,
            text=True,
        )
        lines = (result.stdout or "").strip().splitlines()
        _name_filter = (operator_container_name or "").lower()
        if _name_filter:
            target = next(
                (ln.strip() for ln in lines if _name_filter in ln.lower()),
                None,
            )
        else:
            target = lines[0].strip() if lines else None
        logger.debug(
            f"尝试在{namespace}内搜寻operator pod...结果: {lines}, operator_container_name: {operator_container_name}"
        )
        if not target:
            logger.warning("[fault/crash] 未找到 operator pod，跳过 crash 注入")
            return False
        kubectl_client.kubectl(
            ["delete", "pod", target, "-n", namespace, "--grace-period=0"]
        )
        logger.info(f"[fault/crash] 已删除 pod: {target}")
        return True
    except Exception as exc:
        logger.warning(f"[fault/crash] 删除 pod 失败: {exc}")
        return False


def _wait_operator_healthy(
    kubectl_client: KubectlClient,
    namespace: str,
    operator_container_name: str,
    max_wait: int = HEALTH_MAX_WAIT,
) -> bool:
    """Poll until the operator pod is Running/Ready or max_wait exceeded."""
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        try:
            result = kubectl_client.kubectl(
                [
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "--no-headers",
                    "-o",
                    "custom-columns=NAME:.metadata.name,STATUS:.status.phase,READY:.status.containerStatuses[0].ready",
                ],
                capture_output=True,
                text=True,
            )
            lines = (result.stdout or "").strip().splitlines()
            _name_filter = (operator_container_name or "").lower()
            for ln in lines:
                if _name_filter and _name_filter not in ln.lower():
                    continue
                parts = ln.split()
                status = parts[1] if len(parts) > 1 else ""
                ready = parts[2] if len(parts) > 2 else "false"
                if status == "Running" and ready.lower() == "true":
                    return True
        except Exception:
            pass
        time.sleep(HEALTH_POLL_INTERVAL)
    return False


def _seed_snapshot_for_reconnect(
    fault_manager_url: str,
    task_id: str,
    kubectl_client: KubectlClient,
    cr_kind: str,
    namespace: str,
    cr_name: str = "",
) -> None:
    """Fetch the current CR state and store it as the proxy snapshot in the fault
    manager cache.  Called immediately after registering a reconnect/delay task so
    that the new pod's proxy can restore a meaningful stale snapshot even after a
    pod restart wipes the in-process state.
    """
    if not fault_manager_url or not task_id:
        return
    try:
        cmd = ["get", cr_kind, "-n", namespace, "-o", "json"]
        if cr_name:
            cmd = ["get", cr_kind, cr_name, "-n", namespace, "-o", "json"]
        result = kubectl_client.kubectl(cmd, capture_output=True, text=True)
        if result.returncode != 0 or not result.stdout:
            return
        raw = json.loads(result.stdout)

        items = raw.get("items") if raw.get("kind", "").endswith("List") else [raw]
        if not items:
            return

        snapshot: dict = {}
        for item in items:
            meta = item.get("metadata", {})
            kind = item.get("kind", cr_kind)
            key = f"{kind}/{meta.get('namespace', namespace)}/{meta.get('name', '')}"
            snapshot[key] = item
        snapshot_json = json.dumps(snapshot)
        resp = requests.post(
            f"{fault_manager_url}/api/cache/{task_id}",
            json={"resource": snapshot_json},
            timeout=5,
        )
        resp.raise_for_status()
        logger.info(
            f"[fault/reconnect] snapshot cached for task {task_id} ({len(snapshot)} resources)"
        )
    except Exception as exc:
        logger.debug(f"[fault/reconnect] snapshot cache failed: {exc}")


def _register_fault_task(
    fault_manager_url: str,
    fault_type: str,
    cr_kind: str,
    namespace: str,
    name: str = "",
    round_n: int = 0,
    source: str = "auto",
) -> Optional[str]:
    """POST a fault task to the fault manager.  Returns task_id or None."""
    if not fault_manager_url:
        return None
    try:
        payload = {
            "name": f"fault-r{round_n}-{fault_type}",
            "fault_type": fault_type,
            "source": source,
            "resource_kind": cr_kind,
            "resource_namespace": namespace,
        }
        if name:
            payload["resource_name"] = name
        resp = requests.post(
            f"{fault_manager_url}/api/tasks",
            json=payload,
            timeout=5,
        )
        resp.raise_for_status()
        task_id = resp.json().get("id")
        logger.info(f"[fault/{fault_type}] 注册 task: {task_id}")
        return task_id
    except Exception as exc:
        logger.warning(f"[fault/{fault_type}] 注册任务失败: {exc}")
        return None


def run_fault_phase(
    ckpt: dict,
    ckpt_path: str,
    kubectl_client: KubectlClient,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    seed_cr: dict,
    cr_kind: str,
    branch_meta_index: dict,
    fault_types: List[str],
    fault_manager_url: str = "",
    max_rounds: int = 0,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    steady_wait_sec: int = 480,
    instrument_prefix: str = "",
    rebuild_cluster_fn=None,
    field_relations: Optional[dict] = None,
    declared_field_paths: Optional[set] = None,
    db_dir: str = "",
) -> None:
    """Fault-injection test loop using testplan test cases as CR input pool."""
    logger.info("=" * 70)
    logger.info("Fault-Injection Testing Phase")
    logger.info(f"  fault_types = {fault_types}")
    logger.info("=" * 70)
    update_status(phase="Fault", current_op="初始化")


    ft = ckpt.setdefault(
        "fault_test",
        {
            "rounds": 0,
            "fault_counts": {},
            "failures": 0,
            "results": [],
        },
    )
    fault_counts: Dict[str, int] = ft.get("fault_counts", {})
    for ftype in fault_types:
        fault_counts.setdefault(ftype, 0)


    tp = ckpt.get("testplan", {})
    testcases: dict = tp.get("testcases", {})
    if not testcases:
        logger.error("[fault] 未找到测试用例（需先运行 testplan 模式）")
        return

    tc_list = list(testcases.values())
    rng = _random.Random()
    round_n: int = ft.get("rounds", 0)
    failures: int = ft.get("failures", 0)
    stuck_count = 0

    logger.info(f"[fault] 加载测试用例: {len(tc_list)} 个")
    logger.info(f"[fault] 已完成轮次: {round_n}")


    _db_global_coverage: set = set()
    _db_recorded_count = 0
    if db_dir:
        try:
            from testcase_db.store import _load_index as _db_load_index

            _dbi = _db_load_index(db_dir)
            _db_global_coverage = {int(k) for k in _dbi.keys()}
            logger.info(
                f"[testcase_db] 已加载全局覆盖集合: {len(_db_global_coverage)} 个已覆盖分支"
            )
        except Exception as _dbe:
            logger.warning(f"[testcase_db] 加载失败: {_dbe}")


    _baseline_instr: Optional[dict] = None
    if field_relations is not None and round_n == 0:
        logger.info("[fault] 收集 seed CR 基准插桩数据（用于 inline explore-all）...")
        try:
            _base_instr, _, _base_ok, _, _base_dead = apply_cr_and_collect(
                kubectl_client=kubectl_client,
                namespace=namespace,
                cluster_name=cluster_name,
                input_cr=seed_cr,
                operator_container_name=operator_container_name,
                wait_sec=wait_sec,
                collect_max_wait=collect_max_wait,
                instrument_prefix=instrument_prefix,
            )
            if _base_ok and _base_instr:
                _baseline_instr = _base_instr
                logger.info("[fault] 基准数据收集完成")
            else:
                logger.warning("[fault] 基准数据收集失败，跳过 inline explore-all")
        except Exception as _be:
            logger.warning(f"[fault] 基准收集异常: {_be}")

    while True:
        if max_rounds > 0 and round_n >= max_rounds:
            logger.info(f"[fault] 已达 max_rounds={max_rounds}，退出")
            break


        tc = rng.choice(tc_list)


        chosen_fault = rng.choice(fault_types)


        fault_triggered = False
        if chosen_fault == "crash":
            fault_triggered = _inject_crash(
                kubectl_client, namespace, operator_container_name
            )
            if fault_triggered:
                logger.info("[fault] crash 注入完成，等待 operator 重启...")
                recovered = _wait_operator_healthy(
                    kubectl_client, namespace, operator_container_name
                )
                if not recovered:
                    logger.warning("[fault] operator 未能在超时内恢复，跳过本轮")
                    stuck_count += 1
                    if stuck_count >= STUCK_THRESHOLD:
                        logger.error("[fault] operator 连续无法恢复，终止")
                        break
                    continue
                stuck_count = 0
        elif chosen_fault in ("reconnect", "delay"):


            try:
                _tc_cr = yaml.safe_load(tc.get("cr", "")) or {}
            except Exception:
                _tc_cr = {}
            cr_ns = (
                (_tc_cr.get("metadata") or {}).get("namespace")
                or (seed_cr.get("metadata") or {}).get("namespace")
                or "default"
            )
            task_id = _register_fault_task(
                fault_manager_url,
                chosen_fault,
                cr_kind,
                cr_ns,
                round_n=round_n,
            )
            fault_triggered = task_id is not None
            if fault_triggered:
                _seed_snapshot_for_reconnect(
                    fault_manager_url,
                    task_id,
                    kubectl_client,
                    cr_kind,
                    cr_ns,
                )
            else:
                logger.warning(
                    f"[fault] {chosen_fault} 任务注册失败（fault_manager 不可用），"
                    "本轮仍执行 CR apply"
                )


        try:
            current_cr = yaml.safe_load(tc["cr"])
        except Exception:
            current_cr = seed_cr

        update_status(current_op=f"[r{round_n + 1}] tc={tc['id']} fault={chosen_fault}")
        logger.info(
            f"\n[fault round {round_n + 1}] tc={tc['id']} fault={chosen_fault} "
            f"(triggered={fault_triggered})"
        )

        instr, _, ok, _is_rejection, _cluster_dead = apply_cr_and_collect(
            kubectl_client=kubectl_client,
            namespace=namespace,
            cluster_name=cluster_name,
            input_cr=current_cr,
            operator_container_name=operator_container_name,
            wait_sec=wait_sec,
            collect_max_wait=collect_max_wait,
            instrument_prefix=instrument_prefix,
        )

        if _cluster_dead:
            failures += 1
            logger.error(
                f"[fault] 轮次 {round_n + 1}: operator Pod 不可恢复（fault={chosen_fault}）"
            )
            if rebuild_cluster_fn is not None:
                new_env = rebuild_cluster_fn()
                if new_env:
                    kubectl_client = new_env["kubectl_client"]
                    namespace = new_env["namespace"]
                    cluster_name = new_env["cluster_name"]
                    operator_container_name = new_env["operator_container_name"]
                    logger.info("[fault] 集群已重建，继续测试")
                else:
                    logger.error("[fault] 集群重建失败，终止")
                    break
        elif not ok:
            logger.warning(
                f"[fault] 轮次 {round_n + 1}: apply/collect 失败 (fault={chosen_fault})"
            )
        else:
            if fault_triggered:
                fault_counts[chosen_fault] = fault_counts.get(chosen_fault, 0) + 1


            cr_ns = current_cr.get("metadata", {}).get("namespace", namespace)
            extra_ns = [cr_ns] if cr_ns != namespace else []
            _wait_for_steady_state(
                kubectl_client,
                namespace,
                max_wait_sec=steady_wait_sec,
                extra_namespaces=extra_ns,
            )
            health_issue = _check_cluster_health(
                kubectl_client, namespace, extra_namespaces=extra_ns
            )
            if health_issue:
                failures += 1
                if ft["results"]:
                    ft["results"][-1]["health_issue"] = health_issue
                logger.error(
                    f"[fault] 轮次 {round_n + 1}: 集群健康检查失败 (fault={chosen_fault}): {health_issue}"
                )


            if (
                _baseline_instr is not None
                and field_relations is not None
                and instr is not None
            ):
                try:
                    _rel_diff = diff_branch_sequences(_baseline_instr, instr)
                    _update_field_relations_from_diff(
                        field_relations=field_relations,
                        diff=_rel_diff,
                        cr_before=seed_cr,
                        cr_after=current_cr,
                        mutation_round=f"ft-inline-r{round_n + 1}",
                        branch_meta_index=branch_meta_index,
                        declared_field_paths=declared_field_paths,
                    )
                    ckpt["field_relations"] = field_relations
                except Exception as _rel_exc:
                    logger.debug(f"[fault/inline-explore] 更新失败: {_rel_exc}")


            if db_dir and instr is not None:
                try:
                    _tc_cr_yaml = tc.get("cr", "")
                    from testcase_db.store import record_testcase as _db_record

                    _ftc_id = _db_record(
                        db_dir=db_dir,
                        cr_yaml=_tc_cr_yaml,
                        instr_data=instr,
                        source="fault",
                        global_coverage_set=_db_global_coverage,
                    )
                    if _ftc_id:
                        _db_recorded_count += 1
                        from testcase_db.store import _extract_covered_branches as _ecb

                        for _bi in _ecb(instr):
                            _db_global_coverage.add(_bi)
                        logger.debug(
                            f"[testcase_db] 记录故障用例 {_ftc_id} (tc={tc['id']} fault={chosen_fault})"
                        )
                except Exception as _dbe:
                    logger.debug(f"[testcase_db] 记录失败: {_dbe}")


        round_n += 1
        result_entry = {
            "round": round_n,
            "tc_id": tc["id"],
            "fault_type": chosen_fault,
            "fault_triggered": fault_triggered,
            "apply_ok": ok and not _cluster_dead,
        }
        ft["results"].append(result_entry)
        ft["rounds"] = round_n
        ft["fault_counts"] = fault_counts
        ft["failures"] = failures


        counts_str = "  ".join(f"{k}={v}" for k, v in sorted(fault_counts.items()))
        logger.info(f"  故障统计: {counts_str} | 失败: {failures} | 总轮次: {round_n}")

        update_progress(
            done=round_n,
            total=max_rounds if max_rounds > 0 else round_n + 1,
            label="轮次",
            branches_covered=0,
            branches_total=0,
        )

        _save_checkpoint(ckpt_path, ckpt)


    logger.info("\nFault-Injection Testing 完成:")
    logger.info(f"  总轮次: {round_n}")
    logger.info(f"  故障统计: {fault_counts}")
    logger.info(f"  Operator 不可恢复次数: {failures}")
    if db_dir:
        logger.info(f"  [testcase_db] Fault 阶段共记录 {_db_recorded_count} 个新用例")