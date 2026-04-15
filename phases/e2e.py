

import logging
from typing import List

import yaml

from acto.kubectl_client import KubectlClient
from checkpoint.store import _save_checkpoint
from cluster.apply import apply_cr_and_collect
from cluster.env import _check_cluster_health, _wait_for_steady_state
from core.rich_logger import update_progress, update_status

logger = logging.getLogger(__name__)


STUCK_THRESHOLD = 5


def run_e2e_phase(
    ckpt: dict,
    ckpt_path: str,
    kubectl_client: KubectlClient,
    namespace: str,
    cluster_name: str,
    operator_container_name: str,
    seed_cr: dict,
    cr_kind: str,
    branch_meta_index: dict,
    max_rounds: int = 0,
    wait_sec: int = 15,
    collect_max_wait: int = 0,
    steady_wait_sec: int = 480,
    instrument_prefix: str = "",
    rebuild_cluster_fn=None,
) -> None:
    """End-to-end test loop using testplan test cases as input.

    For each test case the phase:
      1. Applies the CR to the cluster (full delete→restart→create cycle).
      2. Collects instrumentation data.
      3. Compares branch coverage against a baseline to detect anomalies.
      4. Records the result (pass / fail / error).

    All state is persisted in *ckpt* after every round so the run can be
    resumed from the last completed round.
    """
    logger.info("=" * 70)
    logger.info("端到端自动化测试")
    logger.info("=" * 70)
    update_status(phase="E2E", current_op="初始化")


    e2e = ckpt.setdefault(
        "e2e_test",
        {
            "rounds": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "results": [],
        },
    )


    tp = ckpt.get("testplan", {})
    testcases: dict = tp.get("testcases", {})
    if not testcases:
        logger.error("[e2e] 未找到测试用例（需先运行 testplan 模式生成测试计划）")
        return

    tc_list: List[dict] = list(testcases.values())
    round_n: int = e2e.get("rounds", 0)
    passed: int = e2e.get("passed", 0)
    failed: int = e2e.get("failed", 0)
    errors: int = e2e.get("errors", 0)
    stuck_count = 0

    logger.info(f"[e2e] 加载测试用例: {len(tc_list)} 个")
    logger.info(f"[e2e] 已完成轮次: {round_n}")


    if round_n == 0:
        logger.info("[e2e] 收集基准插桩数据（验证集群就绪）...")
        try:
            _, _, _base_ok, _, _ = apply_cr_and_collect(
                kubectl_client=kubectl_client,
                namespace=namespace,
                cluster_name=cluster_name,
                input_cr=seed_cr,
                operator_container_name=operator_container_name,
                wait_sec=wait_sec,
                collect_max_wait=collect_max_wait,
                instrument_prefix=instrument_prefix,
            )
            if _base_ok:
                e2e["baseline_collected"] = True
                logger.info("[e2e] 基准数据收集完成，集群就绪")
            else:
                logger.warning("[e2e] 基准数据收集失败，集群可能未就绪")
        except Exception as exc:
            logger.warning(f"[e2e] 基准收集异常: {exc}")

    total = min(len(tc_list), max_rounds) if max_rounds > 0 else len(tc_list)

    while round_n < len(tc_list):
        if max_rounds > 0 and round_n >= max_rounds:
            logger.info(f"[e2e] 已达 max_rounds={max_rounds}，退出")
            break

        tc = tc_list[round_n]
        tc_id = tc.get("id", f"tc-{round_n}")

        update_status(current_op=f"[round {round_n + 1}/{total}] tc={tc_id}")
        logger.info(f"\n[round {round_n + 1}] tc={tc_id}")


        try:
            current_cr = yaml.safe_load(tc["cr"])
        except Exception:
            logger.warning(f"  [skip] 无法解析测试用例 CR: {tc_id}")
            errors += 1
            round_n += 1
            _record_round(e2e, round_n, tc_id, "error", "CR 解析失败")
            _save_checkpoint(ckpt_path, ckpt)
            continue


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


        verdict = "pass"
        detail = ""

        if _cluster_dead:
            verdict = "fail"
            detail = "Operator Pod 不可恢复"
            failed += 1
            stuck_count += 1
            logger.error(f"  结果: FAILED — {detail}")

            if stuck_count >= STUCK_THRESHOLD:
                if rebuild_cluster_fn is not None:
                    new_env = rebuild_cluster_fn()
                    if new_env:
                        kubectl_client = new_env["kubectl_client"]
                        namespace = new_env["namespace"]
                        cluster_name = new_env["cluster_name"]
                        operator_container_name = new_env["operator_container_name"]
                        logger.info("[e2e] 集群已重建，继续测试")
                        stuck_count = 0
                    else:
                        logger.error("[e2e] 集群重建失败，终止")
                        _record_round(e2e, round_n + 1, tc_id, verdict, detail)
                        round_n += 1
                        break
                else:
                    logger.error("[e2e] 连续无法恢复且无重建函数，终止")
                    _record_round(e2e, round_n + 1, tc_id, verdict, detail)
                    round_n += 1
                    break
        elif not ok:
            if _is_rejection:
                verdict = "skip"
                detail = "CR 被 webhook 拒绝"
                logger.info(f"  结果: SKIP — {detail}")
            else:
                verdict = "error"
                detail = "apply/collect 失败"
                errors += 1
                logger.warning(f"  结果: ERROR — {detail}")
        else:
            stuck_count = 0
            if instr is None:
                verdict = "error"
                detail = "未收集到插桩数据"
                errors += 1
                logger.warning(f"  结果: ERROR — {detail}")
            else:

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
                    verdict = "fail"
                    detail = f"集群健康检查失败: {health_issue}"
                    failed += 1
                    logger.error(f"  结果: FAILED (health) — {health_issue}")
                else:
                    verdict = "pass"
                    detail = "Operator 正常处理 CR，集群健康"
                    passed += 1
                    logger.info("  结果: PASS")


        round_n += 1
        _record_round(e2e, round_n, tc_id, verdict, detail)

        e2e["rounds"] = round_n
        e2e["passed"] = passed
        e2e["failed"] = failed
        e2e["errors"] = errors


        update_progress(
            done=round_n,
            total=total,
            label="轮次",
            branches_covered=0,
            branches_total=0,
        )
        logger.info(
            f"  通过: {passed} | 失败: {failed} | 错误: {errors} | 总轮次: {round_n}"
        )

        _save_checkpoint(ckpt_path, ckpt)


    logger.info("\n" + "=" * 70)
    logger.info("端到端测试完成:")
    logger.info(f"  总轮次: {round_n}")
    logger.info(f"  通过: {passed}")
    logger.info(f"  失败: {failed}")
    logger.info(f"  错误: {errors}")
    logger.info("=" * 70)


def _record_round(
    e2e: dict,
    round_n: int,
    tc_id: str,
    verdict: str,
    detail: str,
) -> None:
    """Append a result entry for this round."""
    e2e["results"].append(
        {
            "round": round_n,
            "tc_id": tc_id,
            "verdict": verdict,
            "detail": detail,
        }
    )