import logging
import os
from datetime import datetime
from html import escape as _html_esc

from report.style import REPORT_TOGGLE_JS, full_report_css

logger = logging.getLogger(__name__)


def _compute_report_stats(ckpt: dict, branch_meta_index: dict) -> dict:
    """从 checkpoint 提取报告所需的统计数值。"""
    p1 = ckpt.get("phase1", {})
    p2 = ckpt.get("phase2", {})
    field_relations = ckpt.get("field_relations", {})
    coverage_map_raw = p2.get("coverage_map", {})
    _tp_raw = p2.get("test_plan", [])
    test_plan = _tp_raw if isinstance(_tp_raw, list) else list(_tp_raw.values())
    p1_log = p1.get("mutation_log", [])
    return {
        "p1_log": p1_log,
        "test_plan": test_plan,
        "explore_log": p2.get("explore_log", []),
        "coverage_map_raw": coverage_map_raw,
        "ts_str": ckpt.get("created_at", "")[:19],
        "total_br": len(branch_meta_index) or len(coverage_map_raw),
        "hit_br": sum(1 for vals in coverage_map_raw.values() if vals),
        "both_br": sum(
            1
            for vals in coverage_map_raw.values()
            if "True" in vals and "False" in vals
        ),
        "p1_ok": sum(1 for m in p1_log if m.get("status") == "ok"),
        "n_relations": len(field_relations),
        "tp_baseline": sum(1 for e in test_plan if e.get("source") == "baseline"),
        "tp_mutation": sum(1 for e in test_plan if e.get("source") == "mutation"),
    }


def _build_coverage_matrix_html(branch_meta_index: dict, coverage_map_raw: dict) -> str:
    """生成 coverage 矩阵的 HTML 色块字符串。"""
    cells = ""
    for bi in sorted(branch_meta_index.keys()):
        bm = branch_meta_index[bi]
        vals = set(coverage_map_raw.get(str(bi), []))
        has_t = "True" in vals
        has_f = "False" in vals
        cls = (
            "cm-both"
            if (has_t and has_f)
            else ("cm-true" if has_t else ("cm-false" if has_f else "cm-none"))
        )
        cond = _html_esc((bm.get("Fmt") or bm.get("Raw", ""))[:80])
        func = _html_esc(bm.get("Func", "")[:40])
        cells += (
            f'<span class="cm-cell {cls}" title="b[{bi}] {func}&#10;{cond}"></span>'
        )
    return cells


def _build_phase1_cards_html(p1_log: list) -> str:
    """生成 Phase 1 字段变异结果卡片的 HTML。"""
    cards = ""
    for m in p1_log:
        fp = _html_esc(m.get("field", ""))
        status = m.get("status", "?")
        s_cls = "p1-ok" if status == "ok" else "p1-fail"
        ds = m.get("diff_summary", {})
        if status == "ok":
            detail = (
                f"changed={ds.get('changed', 0)} "
                f"added={ds.get('added', 0)} "
                f"removed={ds.get('removed', 0)}"
            )
        else:
            detail = _html_esc(str(m.get("error", ""))[:120])
        cards += f"""
      <div class="p1-row {s_cls}">
        <span class="p1-field">{fp}</span>
        <span class="p1-status">{status}</span>
        <span class="p1-detail">{detail}</span>
      </div>"""
    return cards


def _build_test_plan_cards_html(test_plan: list) -> str:
    """生成测试计划卡片列表的 HTML。"""
    rows = ""
    for i, tp in enumerate(test_plan):
        tk = _html_esc(tp.get("target_key") or tp.get("combo", ""))
        attempt = tp.get("attempt", "?")
        source = tp.get("source", "mutation")
        rnd = tp.get("mutation_round", "")
        src_badge = (
            '<span style="color:#4ade80">baseline</span>'
            if source == "baseline"
            else f'<span style="color:#fbbf24">mutation r{rnd}</span>'
        )
        targets_str = _html_esc(
            ", ".join(
                f"b[{t['branch_index']}]→{'T' if t['target_value'] else 'F'}"
                for t in tp.get("targets", [])
            )
        )
        cr_yaml = _html_esc(tp.get("cr_yaml", ""))
        rows += f"""
      <div class="plan-card">
        <div class="plan-header" onclick="toggleCard(this)">
          <span class="plan-num">#{i + 1}</span>
          <span class="plan-combo">{tk}</span>
          <span class="plan-targets">{targets_str}</span>
          <span class="plan-attempt">{src_badge} attempt {attempt}</span>
          <span class="fc-toggle">▼</span>
        </div>
        <div class="fc-body">
          <div class="section-label">CR YAML</div>
          <pre class="cr-pre">{cr_yaml}</pre>
        </div>
      </div>"""
    return rows


def _build_attempt_block_html(att: dict, targets: list) -> str:
    """生成单次 attempt 块的 HTML。"""
    n = att.get("n", "?")
    flip_ok = att.get("flip_success", False)
    err = att.get("error", "")
    a_cls = "att-ok" if flip_ok else ("att-err" if err else "att-miss")
    a_lbl = "✓ flip" if flip_ok else ("⚠ error" if err else "✗ miss")
    err_txt = (
        f'<span class="att-err-msg">{_html_esc(str(err)[:200])}</span>' if err else ""
    )
    bv_after = att.get("branch_values_after", {})
    branch_cells = ""
    for t in targets:
        bi_t = t["branch_index"]
        tv_t = t["target_value"]
        got = bv_after.get(str(bi_t), "?")
        hit = got == str(tv_t)
        branch_cells += (
            f'<span class="bv-chip {"bv-hit" if hit else "bv-miss"}">'
            f"b[{bi_t}]={got}</span>"
        )
    prompt_txt = att.get("prompt", "")
    prompt_sec = ""
    if flip_ok and prompt_txt:
        prompt_sec = (
            f'<div class="att-prompt-toggle" onclick="toggleNext(this)">▶ Prompt</div>'
            f'<pre class="cr-pre prompt-pre" style="display:none">{_html_esc(prompt_txt)}</pre>'
        )
    return f"""
          <div class="att-block {a_cls}">
            <div class="att-row">
              <span class="att-n">#{n}</span>
              <span class="att-lbl">{a_lbl}</span>
              {branch_cells}
              {err_txt}
            </div>
            {prompt_sec}
          </div>"""


def _build_explore_log_html(explore_log: list) -> str:
    """生成 Phase 2 探索日志折叠卡片的 HTML。"""
    rows = ""
    for entry in explore_log:
        tk_disp = _html_esc(entry.get("target_key") or entry.get("combo", ""))
        rnd_disp = entry.get("round", "")
        e_success = entry.get("success", False)
        side_cov = entry.get("side_covered", [])
        targets_desc = _html_esc(
            ", ".join(
                f"b[{t['branch_index']}]→{'T' if t['target_value'] else 'F'}"
                for t in entry.get("targets", [])
            )
        )
        succ_cls = "log-success" if e_success else "log-fail"
        succ_txt = "✓ success" if e_success else "✗ failed"
        targets_list = entry.get("targets", [])
        attempts_html = "".join(
            _build_attempt_block_html(att, targets_list)
            for att in entry.get("attempts", [])
        )
        side_cov_html = (
            f'<span style="color:#a78bfa;font-size:11px">+{len(side_cov)} side</span>'
            if side_cov
            else ""
        )
        rnd_html = (
            f'<span style="color:#7c809a">r{rnd_disp}</span> ' if rnd_disp else ""
        )
        rows += f"""
      <div class="log-entry">
        <div class="log-header" onclick="toggleCard(this)">
          <span class="log-combo">{rnd_html}{tk_disp}</span>
          <span class="log-targets">{targets_desc}</span>
          <span class="log-status {succ_cls}">{succ_txt}</span>
          {side_cov_html}
          <span class="fc-toggle">▼</span>
        </div>
        <div class="fc-body">
          {attempts_html}
        </div>
      </div>"""
    return rows


def generate_pipeline_report(
    ckpt: dict, branch_meta_index: dict, instrument_info_path: str, output_path: str
):
    """从 checkpoint 生成 v5 综合 HTML 报告。"""
    stats = _compute_report_stats(ckpt, branch_meta_index)
    matrix_cells = _build_coverage_matrix_html(
        branch_meta_index, stats["coverage_map_raw"]
    )
    p1_cards = _build_phase1_cards_html(stats["p1_log"])
    plan_rows = _build_test_plan_cards_html(stats["test_plan"])
    log_rows = _build_explore_log_html(stats["explore_log"])
    ts_str = stats["ts_str"]
    total_br = stats["total_br"]
    hit_br = stats["hit_br"]
    both_br = stats["both_br"]
    test_plan = stats["test_plan"]
    explore_log = stats["explore_log"]
    p1_log = stats["p1_log"]
    p1_ok = stats["p1_ok"]
    tp_baseline = stats["tp_baseline"]
    tp_mutation = stats["tp_mutation"]
    n_relations = stats["n_relations"]

    _css = full_report_css()
    _plan_class = "plan-header"
    _log_class = "log-header"

    html = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GSOD v5 — 综合测试报告</title>
<style>
{_css}
.plan-card,.log-entry{{border:1px solid var(--bdr);border-radius:var(--rs);margin-bottom:8px;overflow:hidden}}
.plan-header,.log-header{{background:var(--card-s);padding:8px 14px;display:flex;align-items:center;gap:10px;
  cursor:pointer;user-select:none;flex-wrap:wrap;transition:background .15s}}
.plan-header:hover,.log-header:hover{{background:rgba(79,70,229,.04)}}
.plan-num{{font-weight:700;color:var(--warn);min-width:30px}}
.plan-combo{{font-family:'Cascadia Code',Consolas,monospace;color:var(--acc);font-size:12px}}
.plan-targets{{color:var(--tx2);font-size:11px}}
.plan-attempt{{margin-left:auto;color:var(--acc);font-size:11px}}
.prompt-pre{{margin-top:4px}}
</style></head><body>
<div class="rpt-header">
  <h1>GSOD v5 — 综合测试报告</h1>
  <span class="sub">{_html_esc(ts_str)}</span>
</div>
<div class="rpt-main">

  <div class="stats-row">
    <div class="stat-card"><div class="stat-val">{total_br}</div><div class="stat-label">总分支数</div></div>
    <div class="stat-card"><div class="stat-val">{hit_br}</div><div class="stat-label">已覆盖 (≥1)</div></div>
    <div class="stat-card"><div class="stat-val" style="color:var(--ok)">{both_br}</div><div class="stat-label">双值覆盖 T+F</div></div>
    <div class="stat-card"><div class="stat-val" style="color:var(--warn)">{len(test_plan)}</div><div class="stat-label">测试计划数</div></div>
    <div class="stat-card"><div class="stat-val" style="color:var(--ok)">{tp_baseline}</div><div class="stat-label">基线覆盖</div></div>
    <div class="stat-card"><div class="stat-val" style="color:var(--warn)">{tp_mutation}</div><div class="stat-label">变异覆盖</div></div>
    <div class="stat-card"><div class="stat-val">{n_relations}</div><div class="stat-label">字段关联数</div></div>
    <div class="stat-card"><div class="stat-val">{p1_ok}/{len(p1_log)}</div><div class="stat-label">Phase1 成功</div></div>
  </div>

  <h2>覆盖矩阵</h2>
  <div class="card"><div class="cm-grid">{matrix_cells}</div></div>

  <h2>Phase 1 — 关联分析 ({len(p1_log)} 字段)</h2>
  <div class="card">{'<div class="empty-msg">无记录</div>' if not p1_log else p1_cards}</div>

  <h2>Phase 2 — 测试计划 ({len(test_plan)} 个成功)</h2>
  <div>{'<div class="empty-msg">无计划</div>' if not test_plan else plan_rows}</div>

  <h2>Phase 2 — 探索日志 ({len(explore_log)} 组合)</h2>
  <div>{'<div class="empty-msg">无日志</div>' if not explore_log else log_rows}</div>

  <footer>生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} · instrument_info: {_html_esc(instrument_info_path)}</footer>
</div>

<script>
{REPORT_TOGGLE_JS}
document.querySelectorAll('.plan-header').forEach(h => {{
  h.classList.add('open');
  h.nextElementSibling.classList.add('open');
}});
document.querySelectorAll('.log-header').forEach(h => {{
  const lbl = h.querySelector('.log-status');
  if (lbl && lbl.classList.contains('log-success')) {{
    h.classList.add('open');
    h.nextElementSibling.classList.add('open');
  }}
}});
</script>
</body></html>"""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"v5 报告已生成: {output_path}")