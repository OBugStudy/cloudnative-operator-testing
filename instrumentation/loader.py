import json
import logging
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


def load_instrument_info(path: str) -> dict:
    """加载插桩信息"""
    with open(path, "r", encoding="utf-8") as f:
        info = json.load(f)
    bp = info.get("branch_points", [])
    pn = info.get("predicate_nodes", [])
    logger.info(f"加载插桩信息: {len(bp)} branch_points, {len(pn)} predicate_nodes")
    return info


def build_branch_predicate_map(instrument_info: dict) -> Dict[int, List[dict]]:
    """构建 branch_index -> [predicate_node, ...] 映射"""
    mapping: Dict[int, List[dict]] = {}
    for pn in instrument_info.get("predicate_nodes", []):
        bi = pn["branch_index"]
        mapping.setdefault(bi, []).append(pn)
    return mapping


def extract_branch_values(data: dict, all_branch_ids: Set[str]) -> Dict[str, Any]:
    """从收集到的数据中提取分支取值 {branch_id: True/False/'Unknown'}"""
    values = {}
    for bid, info in data.get("branches", {}).items():
        if isinstance(info, dict):
            v = info.get("v", "Unknown")
            if v == 1 or v is True or v == "1":
                values[str(bid)] = True
            elif v == 0 or v is False or v == "0":
                values[str(bid)] = False
            else:
                values[str(bid)] = "Unknown"

    for bid in all_branch_ids:
        if str(bid) not in values:
            values[str(bid)] = "Unknown"
    return values


def extract_predicate_values(data: dict) -> Dict[str, Any]:
    """从收集到的数据中提取 predicate_node 运行时值

    Returns:
        {predicate_node_index_str: value}
        value 可以是具体值（bool/int/string）或 'Unknown'
    """
    values = {}
    for pid, info in data.get("predicates", {}).items():
        if isinstance(info, dict):
            values[str(pid)] = info.get("v", "Unknown")
        else:
            values[str(pid)] = info
    return values


def extract_expression_context(
    data: dict,
    branch_index: int,
    predicate_node_index: int,
    condition_tree: dict = None,
) -> List[dict]:
    """从 expressions 数据中提取某个谓词的子表达式取值，用于丰富 LLM 上下文

    Args:
        data: collect_timed_data 返回的原始数据
        branch_index: 分支索引
        predicate_node_index: 谓词节点索引
        condition_tree: branch_point 的 condition_tree（可选，用于匹配 node id）

    Returns:
        list of dict, 每项:
            nid: 表达式节点 ID
            kind: 表达式类型 (ident/binary/selector/call...)
            type: Go 运行时类型
            value: 运行时值字符串
            skipped: 是否因短路而跳过
    """
    expressions = data.get("expressions", {})
    results = []
    prefix = f"{branch_index}_{predicate_node_index}_"
    for key, expr_info in expressions.items():
        if not isinstance(expr_info, dict):
            continue
        if key.startswith(prefix) or (
            str(expr_info.get("bid")) == str(branch_index)
            and str(expr_info.get("pid")) == str(predicate_node_index)
        ):
            results.append(
                {
                    "nid": expr_info.get("nid", ""),
                    "kind": expr_info.get("kind", ""),
                    "type": expr_info.get("type", ""),
                    "value": expr_info.get("value", "Unknown"),
                    "skipped": expr_info.get("skipped", False),
                    "skip_reason": expr_info.get("skip_reason", ""),
                }
            )
    return results