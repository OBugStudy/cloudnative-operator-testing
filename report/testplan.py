

import json
import logging
import os
from datetime import datetime
from html import escape as _esc
from string import Template as _Template
from typing import Optional

from report.style import (
    REPORT_CHART_JS_CONFIG,
    REPORT_SORTABLE_JS,
    full_report_css,
)

logger = logging.getLogger(__name__)


def generate_testplan_report(
    ckpt: dict,
    output_path: str,
    branch_meta_index: Optional[dict] = None,
) -> None:
    """Generate a self-contained HTML report from a testplan checkpoint.

    Args:
        ckpt: Loaded checkpoint dict (from checkpoint/store._load_checkpoint).
        output_path: Absolute path to write the .html file.
        branch_meta_index: Optional {branch_index: meta_dict} for branch labels.
    """
    tp = ckpt.get("testplan", {})
    coverage_map: dict = tp.get("coverage_map", {})
    testcases: dict = tp.get("testcases", {})
    targets: dict = tp.get("targets", {})
    branch_history: list = tp.get("branch_coverage_history", [])
    target_history: list = tp.get("target_coverage_history", [])
    bmi = branch_meta_index or {}


    total_branches = len(coverage_map)
    covered_branches = sum(1 for v in coverage_map.values() if v)
    total_targets = len(targets)
    resolved_targets = sum(1 for t in targets.values() if t.get("resolved"))
    total_testcases = len(testcases)
    total_rounds = len(branch_history)
    created_at = ckpt.get("created_at", "")
    updated_at = ckpt.get("updated_at", "")

    branch_pct = int(100 * covered_branches / total_branches) if total_branches else 0
    target_pct = int(100 * resolved_targets / total_targets) if total_targets else 0


    llm_stats = tp.get("llm_stats", {})
    llm_attempts = llm_stats.get("cr_gen_attempts", 0)
    llm_produced = llm_stats.get("cr_gen_produced", 0)
    llm_applied = llm_stats.get("cr_apply_success", 0)
    tgt_hit_stats = tp.get("target_hit_stats", {})
    tgt_attempts = tgt_hit_stats.get("attempts", 0)
    tgt_hits = tgt_hit_stats.get("hits", 0)

    def _rate(num: int, den: int) -> str:
        if den == 0:
            return "— / 0"
        return f"{num} / {den} ({100 * num / den:.1f}%)"

    llm_section_html = _build_llm_stats_html(
        llm_attempts, llm_produced, llm_applied, tgt_attempts, tgt_hits
    )


    cov_rows = []
    for bi_str in sorted(
        coverage_map.keys(), key=lambda x: int(x) if x.isdigit() else 0
    ):
        bi = int(bi_str) if bi_str.isdigit() else bi_str
        covered = coverage_map[bi_str]
        meta = bmi.get(bi, bmi.get(str(bi), {}))
        cond = _esc((meta.get("Fmt") or meta.get("Raw") or "")[:80])
        func = _esc(meta.get("Func", ""))
        file_ = _esc(f"{meta.get('File', '')}:{meta.get('Line', '')}")
        status_cls = "tp-ok" if covered else "tp-miss"
        status_lbl = "✓" if covered else "✗"

        t_res = _target_summary_for_branch(bi_str, targets)
        cov_rows.append(
            f"<tr>"
            f'<td class="mono">{_esc(str(bi))}</td>'
            f'<td class="{status_cls}">{status_lbl}</td>'
            f"<td>{t_res}</td>"
            f"<td>{cond}</td>"
            f"<td>{func}</td>"
            f"<td>{file_}</td>"
            f"</tr>"
        )
    cov_table = (
        '<table class="tp-tbl sortable" id="cov-table">'
        "<thead><tr>"
        "<th>Branch</th><th>Seen</th><th>Targets</th>"
        "<th>Condition</th><th>Func</th><th>File:Line</th>"
        "</tr></thead>"
        f"<tbody>{''.join(cov_rows)}</tbody></table>"
    )


    tgt_rows = []
    for key in sorted(targets.keys()):
        tgt = targets[key]
        resolved = tgt.get("resolved", False)
        tc_ids = tgt.get("testcase_id", [])
        parts = key.split("_")
        try:
            bi = int(parts[0])
            want_lbl = parts[1]
        except (IndexError, ValueError):
            bi, want_lbl = key, ""
        meta = bmi.get(bi, bmi.get(str(bi), {}))
        cond = _esc((meta.get("Fmt") or meta.get("Raw") or "")[:80])
        status_cls = "tp-ok" if resolved else "tp-miss"
        status_lbl = "✓ Resolved" if resolved else "✗ Open"
        tc_str = ", ".join(str(x) for x in tc_ids) if tc_ids else "—"
        tgt_rows.append(
            f"<tr>"
            f'<td class="mono">{_esc(key)}</td>'
            f'<td class="mono">{_esc(str(bi))}</td>'
            f'<td class="mono">{_esc(want_lbl)}</td>'
            f'<td class="{status_cls}">{status_lbl}</td>'
            f'<td class="mono">{_esc(tc_str)}</td>'
            f"<td>{cond}</td>"
            f"</tr>"
        )
    tgt_table = (
        '<table class="tp-tbl sortable" id="tgt-table">'
        "<thead><tr>"
        "<th>Key</th><th>Branch</th><th>Value</th>"
        "<th>Status</th><th>TestCase IDs</th><th>Condition</th>"
        "</tr></thead>"
        f"<tbody>{''.join(tgt_rows)}</tbody></table>"
    )


    tc_rows = []
    for tc_id in sorted(testcases.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        tc = testcases[tc_id]
        freq = tc.get("frequency", 0)
        hnb = tc.get("has_new_branch", True)
        badge = (
            '<span class="badge-new">new-branch</span>'
            if hnb
            else '<span class="badge-tgt">target-only</span>'
        )
        branches = ", ".join(str(b) for b in tc.get("involved_branches", [])[:20])
        if len(tc.get("involved_branches", [])) > 20:
            branches += f" +{len(tc['involved_branches']) - 20} more"
        cr_yaml = _esc(tc.get("cr", ""))
        tc_rows.append(
            f"<tr>"
            f'<td class="mono">{_esc(tc_id)}</td>'
            f"<td>{badge}</td>"
            f'<td class="mono">{freq}</td>'
            f'<td class="mono small">{_esc(branches)}</td>'
            f"<td><details><summary>show CR</summary>"
            f'<pre class="cr-pre">{cr_yaml}</pre></details></td>'
            f"</tr>"
        )
    tc_table = (
        '<table class="tp-tbl" id="tc-table">'
        "<thead><tr>"
        "<th>ID</th><th>Type</th><th>Freq</th><th>Involved Branches</th><th>CR</th>"
        "</tr></thead>"
        f"<tbody>{''.join(tc_rows)}</tbody></table>"
    )


    branch_chart_data = _build_history_chart_data(branch_history, "total_covered")
    target_chart_data = _build_history_chart_data(target_history, "total_resolved")
    branch_chart_js = json.dumps(branch_chart_data)
    target_chart_js = json.dumps(target_chart_data)


    bh_rows = []
    for entry in branch_history[:200]:
        r = entry.get("round", "")
        tc = entry.get("testcase_id", "")
        newly = ", ".join(
            str(x) for x in (entry.get("newly_covered_branches") or [])[:10]
        )
        total = entry.get("total_covered", "")
        bh_rows.append(
            f"<tr><td>{r}</td><td class='mono'>{_esc(str(tc))}</td>"
            f"<td class='mono small'>{_esc(newly)}</td><td>{total}</td></tr>"
        )
    bh_table = (
        '<table class="tp-tbl" id="bh-table">'
        "<thead><tr><th>Round</th><th>TestCase</th><th>Newly Covered Branches</th><th>Total Covered</th></tr></thead>"
        f"<tbody>{''.join(bh_rows)}</tbody></table>"
        + (
            '<p class="muted">（仅显示前 200 条）</p>'
            if len(branch_history) > 200
            else ""
        )
    )


    th_rows = []
    for entry in target_history[:200]:
        r = entry.get("round", "")
        tc = entry.get("testcase_id", "")
        newly = ", ".join(
            str(x) for x in (entry.get("newly_resolved_targets") or [])[:10]
        )
        total = entry.get("total_resolved", "")
        th_rows.append(
            f"<tr><td>{r}</td><td class='mono'>{_esc(str(tc))}</td>"
            f"<td class='mono small'>{_esc(newly)}</td><td>{total}</td></tr>"
        )
    th_table = (
        '<table class="tp-tbl" id="th-table">'
        "<thead><tr><th>Round</th><th>TestCase</th><th>Newly Resolved Targets</th><th>Total Resolved</th></tr></thead>"
        f"<tbody>{''.join(th_rows)}</tbody></table>"
        + (
            '<p class="muted">（仅显示前 200 条）</p>'
            if len(target_history) > 200
            else ""
        )
    )


    html = _Template(_HTML_TEMPLATE).safe_substitute(
        gen_time=_esc(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        created_at=_esc(created_at),
        updated_at=_esc(updated_at),
        total_branches=total_branches,
        covered_branches=covered_branches,
        branch_pct=branch_pct,
        total_targets=total_targets,
        resolved_targets=resolved_targets,
        target_pct=target_pct,
        total_testcases=total_testcases,
        total_rounds=total_rounds,
        llm_attempts=llm_attempts,
        llm_produced=llm_produced,
        llm_applied=llm_applied,
        llm_gen_rate=_rate(llm_produced, llm_attempts),
        llm_apply_rate=_rate(llm_applied, llm_produced),
        tgt_hit_rate=_rate(tgt_hits, tgt_attempts),
        llm_section=llm_section_html,
        cov_table=cov_table,
        tgt_table=tgt_table,
        tc_table=tc_table,
        bh_table=bh_table,
        th_table=th_table,
        branch_chart_js=branch_chart_js,
        target_chart_js=target_chart_js,
    )

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"[testplan report] 已生成: {output_path}")


def _target_summary_for_branch(bi_str: str, targets: dict) -> str:
    """Return a compact HTML badge showing T/F target status for a branch."""
    parts = []
    for suffix, label in (("T", "T"), ("F", "F")):
        key = f"{bi_str}_{suffix}"
        if key not in targets:
            continue
        resolved = targets[key].get("resolved", False)
        cls = "tp-ok" if resolved else "tp-miss"
        parts.append(f'<span class="{cls} badge">{label}</span>')
    return " ".join(parts) if parts else "—"


def _build_history_chart_data(history: list, value_key: str) -> dict:
    """Build {labels, values} for a simple line chart."""
    labels = []
    values = []
    for entry in history:
        labels.append(entry.get("round", len(labels)))
        values.append(entry.get(value_key, 0))
    return {"labels": labels, "values": values}


def _build_llm_stats_html(
    llm_attempts: int,
    llm_produced: int,
    llm_applied: int,
    tgt_attempts: int,
    tgt_hits: int,
) -> str:
    """Return an HTML card showing LLM CR generation and target hit stats."""
    if llm_attempts == 0 and tgt_attempts == 0:
        return '<p class="muted">（本次运行未使用 LLM 模式，无统计数据）</p>'

    def _r(num: int, den: int) -> str:
        if den == 0:
            return '<span class="tp-miss">— / 0</span>'
        pct = 100 * num / den
        cls = "tp-ok" if pct >= 50 else "tp-miss"
        return f'<span class="{cls}">{num} / {den} ({pct:.1f}%)</span>'

    rows = [
        ("LLM 调用次数", f"<b>{llm_attempts}</b>"),
        ("CR 生成成功率", _r(llm_produced, llm_attempts)),
        ("CR 应用成功率", _r(llm_applied, llm_produced)),
        ("--- 目标命中率", _r(tgt_hits, tgt_attempts)),
    ]
    inner = "".join(
        f'<div class="llm-row"><span class="llm-lbl">{lbl}</span><span class="llm-val">{val}</span></div>'
        for lbl, val in rows
    )
    return f'<div class="llm-card">{inner}</div>'


_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GSOD 测试计划覆盖报告</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
"""
    + full_report_css()
    + """
.tp-tbl{width:100%;border-collapse:collapse;font-size:13px}
.tp-tbl th{background:rgba(79,70,229,.04);padding:10px 12px;text-align:left;color:var(--tx2);font-size:11px;
  font-weight:600;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid var(--bdr);
  white-space:nowrap;cursor:pointer;user-select:none}
.tp-tbl th:hover{color:var(--acc)}
.tp-tbl td{padding:8px 12px;border-bottom:1px solid rgba(200,210,225,.35);vertical-align:top}
.tp-tbl tbody tr:hover{background:rgba(79,70,229,.03)}
</style>
</head>
<body>
<div class="rpt-header">
  <h1>GSOD 测试计划覆盖报告</h1>
  <span class="sub">生成于 $gen_time &nbsp;|&nbsp; 创建 $created_at &nbsp;|&nbsp; 更新 $updated_at</span>
</div>
<div class="rpt-main">

<!-- Summary cards -->
<div class="stats-row">
  <div class="stat-card">
    <div class="stat-val tp-ok">$covered_branches<span style="font-size:14px;color:var(--tx2)">/$total_branches</span></div>
    <div class="stat-label">分支覆盖</div>
    <div class="stat-bar"><div class="stat-bar-fill" style="width:${branch_pct}%"></div></div>
  </div>
  <div class="stat-card">
    <div class="stat-val tp-ok">$resolved_targets<span style="font-size:14px;color:var(--tx2)">/$total_targets</span></div>
    <div class="stat-label">目标解决</div>
    <div class="stat-bar"><div class="stat-bar-fill" style="width:${target_pct}%"></div></div>
  </div>
  <div class="stat-card">
    <div class="stat-val">$total_testcases</div>
    <div class="stat-label">测试用例池</div>
  </div>
  <div class="stat-card">
    <div class="stat-val">$total_rounds</div>
    <div class="stat-label">总轮次</div>
  </div>
  <div class="stat-card">
    <div class="stat-val">$llm_attempts</div>
    <div class="stat-label">LLM 调用</div>
  </div>
  <div class="stat-card">
    <div class="stat-val">$llm_produced<span style="font-size:14px;color:var(--tx2)">/$llm_attempts</span></div>
    <div class="stat-label">LLM CR 生成</div>
  </div>
  <div class="stat-card">
    <div class="stat-val">$llm_applied<span style="font-size:14px;color:var(--tx2)">/$llm_produced</span></div>
    <div class="stat-label">LLM CR 应用</div>
  </div>
</div>

<!-- LLM stats detail -->
<h2>LLM 统计</h2>
$llm_section

<!-- Charts -->
<div class="chart-wrap">
  <div class="chart-card">
    <h3>分支覆盖趋势</h3>
    <canvas id="branchChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>目标解决趋势</h3>
    <canvas id="targetChart"></canvas>
  </div>
</div>

<!-- Tabs -->
<div class="tab-nav">
  <button class="tab-btn active" onclick="showTab('branches',this)">分支 ($total_branches)</button>
  <button class="tab-btn" onclick="showTab('targets',this)">目标 ($total_targets)</button>
  <button class="tab-btn" onclick="showTab('testcases',this)">测试用例 ($total_testcases)</button>
  <button class="tab-btn" onclick="showTab('bhistory',this)">分支历史</button>
  <button class="tab-btn" onclick="showTab('thistory',this)">目标历史</button>
  <button class="tab-btn" onclick="showTab('llmtab',this)">目标命中率: $tgt_hit_rate</button>
</div>

<div id="branches" class="tab-panel active">
  <h2>分支覆盖</h2>
  $cov_table
</div>
<div id="targets" class="tab-panel">
  <h2>测试目标</h2>
  $tgt_table
</div>
<div id="testcases" class="tab-panel">
  <h2>测试用例池</h2>
  $tc_table
</div>
<div id="bhistory" class="tab-panel">
  <h2>分支覆盖历史</h2>
  $bh_table
</div>
<div id="thistory" class="tab-panel">
  <h2>目标解决历史</h2>
  $th_table
</div>
<div id="llmtab" class="tab-panel">
  <h2>LLM 统计明细</h2>
  $llm_section
</div>

</div><!-- /rpt-main -->

<script>
function showTab(id, btn) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
}

"""
    + REPORT_CHART_JS_CONFIG
    + """

const bd = $branch_chart_js;
new Chart(document.getElementById('branchChart'), {
  ...chartDefaults,
  data: {
    labels: bd.labels,
    datasets: [{ data: bd.values, borderColor: '#4f46e5', backgroundColor: 'rgba(79,70,229,.08)',
                  pointRadius: 2, tension: 0.3, fill: true }]
  }
});

const td2 = $target_chart_js;
new Chart(document.getElementById('targetChart'), {
  ...chartDefaults,
  data: {
    labels: td2.labels,
    datasets: [{ data: td2.values, borderColor: '#059669', backgroundColor: 'rgba(5,150,105,.08)',
                  pointRadius: 2, tension: 0.3, fill: true }]
  }
});

"""
    + REPORT_SORTABLE_JS
    + """
</script>
</body>
</html>
"""
)