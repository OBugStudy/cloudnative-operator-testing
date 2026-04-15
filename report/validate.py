

import json
import logging
import os
from html import escape as _html_esc
from typing import Optional

from report.explore_all import (
    _branch_compare_table,
    _cr_yaml_diff_html,
)
from report.style import full_report_css

logger = logging.getLogger(__name__)


def _build_validate_detail(
    res: dict,
    row_id: str,
    branch_meta_index: Optional[dict],
) -> str:
    """Build the expandable detail panel for one validate result row."""
    parts = []


    status = res.get("replay_status", "?")
    if status == "error":
        err = _html_esc(str(res.get("error", "(unknown error)")))
        parts.append(
            f'<div class="det-section"><div class="det-title">错误信息</div>'
            f'<pre class="det-pre err-pre">{err}</pre></div>'
        )

    if status == "skip" or status == "dry-run":
        reason = _html_esc(res.get("skip_reason", ""))
        parts.append(
            f'<div class="det-section"><div class="det-title">跳过原因</div>'
            f'<p class="v-none">{reason or "(未知)"}</p></div>'
        )


    if res.get("correction_triggered"):
        correction = res.get("correction") or {}
        if correction.get("corrected") and correction.get("new_result"):
            nr = correction["new_result"]
            nr_d = nr.get("replay_diff", {})
            nr_bis = nr.get("recorded_branch_indices", [])
            bi_tags = " ".join(
                f'<span class="bi-tag">b[{b}]</span>' for b in sorted(nr_bis)[:10]
            )
            corr_yaml_id = f"corr-cr-{row_id}"
            c_b_yaml = _html_esc(nr.get("base_cr_yaml", "") or "(无)")
            c_a_yaml = _html_esc(nr.get("mutated_cr_yaml", "") or "(无)")
            parts.append(
                f'<div class="det-section corr-section">'
                f'<div class="det-title" style="color:#7c3aed">⚡ 修正重探测（trace 异常，已自动执行）</div>'
                f'<div class="corr-summary">'
                f'  <span class="badge-chg">↗ changed={nr_d.get("changed", 0)}</span> '
                f'  <span class="badge-add">↗ added={nr_d.get("added", 0)}</span> '
                f'  <span class="badge-rem">↗ removed={nr_d.get("removed", 0)}</span>'
                f'  <span style="margin-left:12px">新增关联: {bi_tags or "(无)"}'
                f"</span></div>"
                f'<div class="det-title" style="cursor:pointer;font-size:12px;margin-top:8px" '
                f"onclick=\"toggleEl('{corr_yaml_id}')\">▶ 修正 CR YAML（展开查看）</div>"
                f'<div id="{corr_yaml_id}" style="display:none">'
                f"{_cr_yaml_diff_html(nr.get('base_cr_yaml', ''), nr.get('mutated_cr_yaml', ''))}"
                f'<div class="expr-cmp" style="margin-top:8px">'
                f'  <div class="expr-col"><div class="expr-col-hdr">修正用 base CR</div>'
                f'  <pre class="det-pre">{c_b_yaml}</pre></div>'
                f'  <div class="expr-col"><div class="expr-col-hdr">修正得到的 mutated CR</div>'
                f'  <pre class="det-pre">{c_a_yaml}</pre></div>'
                f"</div></div></div>"
            )
        else:
            corr_err = _html_esc(correction.get("error", "(未知)"))
            parts.append(
                f'<div class="det-section corr-section">'
                f'<div class="det-title" style="color:var(--warn)">⚠ 修正尝试失败</div>'
                f'<pre class="det-pre err-pre">{corr_err}</pre></div>'
            )


    targeted = res.get("field", "")
    cr_chg = res.get("cr_changed_fields", [])
    if cr_chg:
        chg_items = "".join(
            f'<span class="cr-chg-field{" cr-chg-extra" if fp2 != targeted else ""}">'
            f"{_html_esc(fp2)}</span>"
            for fp2 in cr_chg
        )
        note = (
            " <span class='v-none'>(★ = 目标字段, 黄色 = 额外改变)</span>"
            if any(fp2 != targeted for fp2 in cr_chg)
            else ""
        )
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title">实际变化的 CR 字段{note}</div>'
            f'<div class="cr-chg-list">{chg_items}</div></div>'
        )


    base_cr_yaml = res.get("base_cr_yaml", "")
    mutated_cr_yaml = res.get("mutated_cr_yaml", "")
    if base_cr_yaml or mutated_cr_yaml:
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title">CR 变更 diff（变异前 → 变异后）</div>'
            f"{_cr_yaml_diff_html(base_cr_yaml, mutated_cr_yaml)}</div>"
        )
        full_cr_id = f"full-cr-{row_id}"
        b_yaml_esc = _html_esc(base_cr_yaml or "(无)")
        a_yaml_esc = _html_esc(mutated_cr_yaml or "(无)")
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title" style="cursor:pointer" onclick="toggleEl(\'{full_cr_id}\')">'
            f"▶ 完整 CR YAML（变异前 / 变异后）</div>"
            f'<div id="{full_cr_id}" style="display:none">'
            f'<div class="expr-cmp">'
            f'  <div class="expr-col">'
            f'    <div class="expr-col-hdr">变异前 (base CR)</div>'
            f'    <pre class="det-pre">{b_yaml_esc}</pre>'
            f"  </div>"
            f'  <div class="expr-col">'
            f'    <div class="expr-col-hdr">变异后 (mutated CR)</div>'
            f'    <pre class="det-pre">{a_yaml_esc}</pre>'
            f"  </div>"
            f"</div></div></div>"
        )


    before_instr = res.get("before_instr")
    after_instr = res.get("after_instr")
    diff_raw = res.get("diff_raw") or {}
    if after_instr:
        baseline_traces: dict = {
            t["branch_index"]: t
            for t in (before_instr or {}).get("traces", [])
            if isinstance(t, dict)
        }
        cmp_table = _branch_compare_table(
            after_instr, diff_raw, baseline_traces, branch_meta_index
        )
        parts.append(
            '<div class="det-section">'
            '<div class="det-title">采集器数据对比（变更前 before vs 变更后 after）</div>'
            '<p class="det-hint">点击 branch 行展开表达式/变量详情；'
            "黄色=值变化，绿色=新增，红色=消失</p>" + cmp_table + "</div>"
        )


    if before_instr or after_instr:
        raw_id = f"raw-{row_id}"
        b_json = _html_esc(json.dumps(before_instr or {}, ensure_ascii=False, indent=2))
        a_json = _html_esc(json.dumps(after_instr or {}, ensure_ascii=False, indent=2))
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title" style="cursor:pointer" onclick="toggleEl(\'{raw_id}\')">'
            f"▶ 原始 JSON 数据（调试用）</div>"
            f'<div id="{raw_id}" style="display:none">'
            f'<div class="expr-cmp">'
            f'  <div class="expr-col"><div class="expr-col-hdr">before_instr (JSON)</div>'
            f'  <pre class="det-pre">{b_json}</pre></div>'
            f'  <div class="expr-col"><div class="expr-col-hdr">after_instr (JSON)</div>'
            f'  <pre class="det-pre">{a_json}</pre></div>'
            f"</div></div></div>"
        )

    if not parts:
        return ""
    return (
        f'<tr id="det-{row_id}" class="det-row" style="display:none">'
        f'<td colspan="7"><div class="det-wrap">{"".join(parts)}</div></td></tr>'
    )


def generate_validate_report(
    report: dict,
    output_path: str,
    branch_meta_index: Optional[dict] = None,
    ea_checkpoint_path: str = "",
) -> None:
    """Generate an HTML validation report from the validate mode result dict."""
    results = report.get("results", [])
    summary = report.get("summary", {})
    fields_requested = report.get("fields_requested", [])
    fields_missing = report.get("fields_missing", [])

    n_ok = summary.get("ok", 0)
    n_corrected = summary.get("ok_corrected", 0)
    n_unhealthy = summary.get("ok_unhealthy", 0)
    n_err = summary.get("error", 0)
    n_skip = summary.get("skip", 0)
    n_rel = summary.get("new_branch_relations", 0)
    n_total = len(results)


    rows_html = ""
    for idx, res in enumerate(results):
        row_id = str(idx)
        fp = _html_esc(res.get("field", ""))
        sub_kind = _html_esc(res.get("sub_kind", ""))
        status = res.get("replay_status", "?")
        skip_reason = _html_esc(res.get("skip_reason", ""))

        if status == "ok":
            s_cls = "ea-ok"
            s_html = '<span class="ok-mark">✓</span>'
        elif status == "ok_corrected":
            s_cls = "ea-ok"
            s_html = '<span class="ok-mark">✓</span> <span class="badge-corrected">修正</span>'
        elif status == "ok_unhealthy":
            s_cls = ""
            s_html = '<span class="badge-unhealthy">⚠ 修正失败</span>'
        elif status == "error":
            s_cls = "ea-fail"
            s_html = '<span class="fail-mark">✗</span>'
        elif status == "dry-run":
            s_cls = ""
            s_html = '<span class="badge" style="background:#e0e7ff;color:var(--acc)">dry-run</span>'
        else:
            s_cls = ""
            s_html = f'<span class="v-none">{_html_esc(status)}</span>'


        rd = res.get("replay_diff", {})
        if status in ("ok", "ok_corrected", "ok_unhealthy"):
            nc, na, nr = rd.get("changed", 0), rd.get("added", 0), rd.get("removed", 0)
            diff_parts = []
            if nc:
                diff_parts.append(f'<span class="badge-chg">⇄{nc}</span>')
            if na:
                diff_parts.append(f'<span class="badge-add">＋{na}</span>')
            if nr:
                diff_parts.append(f'<span class="badge-rem">－{nr}</span>')
            diff_html = (
                " ".join(diff_parts)
                if diff_parts
                else '<span class="v-none">无 diff</span>'
            )
        elif skip_reason:
            diff_html = f'<span class="v-none">{skip_reason[:80]}</span>'
        else:
            diff_html = (
                f'<span class="err-txt">{_html_esc(res.get("error", ""))[:80]}</span>'
            )


        bv = _html_esc(str(res.get("field_before") or "—"))
        av = _html_esc(str(res.get("field_after") or "—"))
        val_html = f'<span class="v-none">{bv}</span> → <b>{av}</b>'


        rec_bis = res.get("recorded_branch_indices", [])
        if rec_bis:
            bi_parts = []
            for b in sorted(rec_bis)[:10]:
                bm = (branch_meta_index or {}).get(b, {})
                cond_short = (bm.get("Fmt") or bm.get("Raw") or "")[:50]
                tip = f' title="{_html_esc(cond_short)}"' if cond_short else ""
                bi_parts.append(f'<span class="bi-tag"{tip}>b[{b}]</span>')
            rel_html = " ".join(bi_parts)
            if len(rec_bis) > 10:
                rel_html += f'<span class="v-none"> +{len(rec_bis) - 10}</span>'
            rel_badge = '<span class="rel-yes">✓ 关联</span>'
        else:
            rel_html = ""
            rel_badge = '<span class="rel-no">— 无关联</span>'

        expand_btn = (
            f'<button class="exp-btn" onclick="event.stopPropagation();'
            f"toggleDet('{row_id}')\">▶ 详情</button>"
        )

        rows_html += f"""
      <tr class="{s_cls}" onclick="toggleDet('{row_id}')" style="cursor:pointer">
        <td class="fp">{fp} {expand_btn}</td>
        <td class="mono">{sub_kind}</td>
        <td>{s_html}</td>
        <td>{val_html}</td>
        <td>{diff_html}</td>
        <td>{rel_badge}</td>
        <td class="branches">{rel_html}</td>
      </tr>"""
        rows_html += _build_validate_detail(res, row_id, branch_meta_index)


    missing_html = ""
    if fields_missing:
        items = "".join(
            f'<span class="cr-chg-extra">{_html_esc(fp)}</span> '
            for fp in fields_missing
        )
        missing_html = (
            f'<div class="card" style="border-color:rgba(217,119,6,.3)">'
            f'<b style="color:var(--warn)">⚠ 在 mutation_log 中未找到的字段</b>'
            f'<div class="cr-chg-list" style="margin-top:8px">{items}</div></div>'
        )

    _css = full_report_css()
    src_label = (
        _html_esc(os.path.basename(ea_checkpoint_path)) if ea_checkpoint_path else ""
    )

    html = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Validate 验证报告</title>
<style>
{_css}
  table.main-tbl{{width:100%;border-collapse:collapse;font-size:13px;}}
  table.main-tbl th{{background:rgba(79,70,229,.04);color:var(--tx2);padding:8px 10px;text-align:left;
      font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;
      border-bottom:2px solid var(--bdr);position:sticky;top:0;z-index:2;}}
  table.main-tbl td{{padding:6px 10px;border-bottom:1px solid rgba(200,210,225,.35);vertical-align:middle;}}
  tr.ea-ok:hover > td{{background:rgba(79,70,229,.03);}}
  tr.ea-fail > td{{color:var(--tx3);}}
  tr.ea-fail:hover > td{{background:rgba(220,38,38,.04);}}
  .fp{{color:var(--acc);word-break:break-all;font-family:'Cascadia Code',Consolas,monospace;font-size:12px;}}
  .branches{{line-height:1.8;}}
  .bi-tag{{background:#e0e7ff;border:1px solid rgba(79,70,229,.2);border-radius:4px;
           padding:1px 6px;margin:1px;font-size:11px;color:var(--acc);}}
  .rel-yes{{color:var(--ok);font-weight:bold;}}
  .rel-no{{color:var(--tx3);}}
  .ok-mark{{color:var(--ok);font-weight:bold;font-size:1.1em;}}
  .fail-mark{{color:var(--err);font-weight:bold;font-size:1.1em;}}
  .err-txt{{color:var(--err);font-size:12px;}}
  .det-row > td{{padding:0;border-bottom:2px solid rgba(79,70,229,.15);}}
  .det-wrap{{background:#f8fafc;padding:16px 20px;border-left:3px solid var(--acc);}}
  .det-section{{margin-bottom:16px;}}
  .det-title{{color:var(--acc);font-weight:bold;font-size:13px;margin-bottom:6px;
              padding:4px 0;border-bottom:1px solid var(--bdr);}}
  .det-hint{{color:var(--tx2);font-size:11px;margin:4px 0 8px;}}
  .det-pre{{background:#fff;border:1px solid var(--bdr);border-radius:6px;
            padding:10px;overflow-x:auto;white-space:pre;font-size:12px;
            color:var(--tx);margin:0;max-height:350px;overflow-y:auto;
            font-family:'Cascadia Code',Consolas,monospace;}}
  .err-pre{{border-color:rgba(220,38,38,.3);color:var(--err);}}
  table.br-tbl{{width:100%;border-collapse:collapse;font-size:12px;margin-top:4px;}}
  table.br-tbl th{{background:rgba(79,70,229,.04);color:var(--tx2);padding:5px 8px;
                   text-align:left;border-bottom:1px solid var(--bdr);}}
  table.br-tbl td{{padding:4px 8px;border-bottom:1px solid rgba(200,210,225,.35);vertical-align:top;}}
  tr.br-changed > td{{background:#fef3c7;}}
  tr.br-added > td{{background:#d1fae5;}}
  tr.br-removed > td{{background:#fee2e2;}}
  tr.br-same > td{{color:var(--tx3);}}
  tr.br-row:hover > td{{filter:brightness(.97);}}
  .bi-cell{{color:var(--warn);font-weight:bold;white-space:nowrap;}}
  .cond-cell{{max-width:500px;}}
  .br-meta{{margin-bottom:4px;}}
  .br-meta code{{color:var(--acc);background:#e0e7ff;padding:1px 5px;border-radius:3px;font-size:11px;}}
  .br-func{{color:var(--tx2);font-size:11px;margin-left:6px;}}
  .badge-chg{{color:var(--warn);background:#fef3c7;border:1px solid rgba(217,119,6,.2);border-radius:4px;padding:1px 6px;font-size:11px;}}
  .badge-add{{color:var(--ok);background:#d1fae5;border:1px solid rgba(5,150,105,.2);border-radius:4px;padding:1px 6px;font-size:11px;}}
  .badge-rem{{color:var(--err);background:#fee2e2;border:1px solid rgba(220,38,38,.2);border-radius:4px;padding:1px 6px;font-size:11px;}}
  .badge-same{{color:var(--tx3);font-size:11px;}}
  .v-true{{color:var(--ok);font-weight:bold;}}
  .v-false{{color:var(--err);font-weight:bold;}}
  .v-none{{color:var(--tx3);font-style:italic;}}
  .expr-row > td{{padding:0;background:#f1f5f9;}}
  .expr-cmp{{display:flex;gap:12px;padding:10px;}}
  .expr-col{{flex:1;min-width:0;}}
  .expr-col-hdr{{color:var(--tx2);font-size:11px;font-weight:bold;text-transform:uppercase;
                 letter-spacing:.05em;margin-bottom:6px;padding-bottom:3px;border-bottom:1px solid var(--bdr);}}
  .expr-block{{background:#fff;border:1px solid var(--bdr);border-radius:6px;padding:8px;margin-bottom:6px;}}
  .expr-hdr{{display:block;margin-bottom:5px;font-size:12px;}}
  .expr-hdr b{{color:var(--acc);}}
  .expr-meta{{color:var(--tx2);font-size:11px;margin-left:8px;}}
  table.var-tbl{{width:100%;border-collapse:collapse;font-size:11px;}}
  table.var-tbl td{{padding:2px 6px;border-bottom:1px solid rgba(200,210,225,.35);}}
  .vi-idx{{color:var(--tx2);width:30px;}}.vi-kind{{color:#7c3aed;width:80px;}}
  .vi-type{{color:var(--tx2);width:100px;}}.vi-val{{color:var(--warn);font-weight:bold;}}
  .exp-btn{{background:none;border:1px solid var(--bdr);color:var(--tx2);border-radius:4px;
            padding:1px 8px;font-size:11px;cursor:pointer;margin-left:6px;vertical-align:middle;}}
  .exp-btn:hover{{background:rgba(79,70,229,.06);color:var(--tx);}}
  .cr-chg-list{{display:flex;flex-wrap:wrap;gap:5px;padding:4px 0;}}
  .cr-chg-field{{background:#e0e7ff;border:1px solid rgba(79,70,229,.2);border-radius:4px;padding:2px 8px;font-size:12px;color:var(--acc);}}
  .cr-chg-extra{{background:#fef3c7;border-color:rgba(217,119,6,.2);color:var(--warn);}}
  .badge-corrected{{background:#ede9fe;border:1px solid rgba(124,58,237,.25);border-radius:4px;
                    padding:1px 7px;font-size:11px;color:#7c3aed;font-weight:600;margin-left:4px;}}
  .badge-unhealthy{{background:#fef3c7;border:1px solid rgba(217,119,6,.3);border-radius:4px;
                    padding:1px 7px;font-size:11px;color:var(--warn);font-weight:600;}}
  .corr-section{{border-left:3px solid #7c3aed!important;background:#faf5ff;border-radius:4px;padding:10px 14px;}}
  .corr-summary{{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px;align-items:center;}}
  .diff-pre{{font-size:12px;line-height:1.4;max-height:400px;overflow-y:auto;}}
  .diff-add{{display:block;background:#d1fae5;color:var(--ok);}}
  .diff-rem{{display:block;background:#fee2e2;color:var(--err);}}
  .diff-hdr{{display:block;color:var(--tx2);background:#f1f5f9;}}
  .diff-ctx{{display:block;color:var(--tx3);}}
</style>
<script>
function toggleDet(id) {{
  var row = document.getElementById('det-' + id);
  if (!row) return;
  var open = row.style.display !== 'none';
  row.style.display = open ? 'none' : '';
  var btn = document.querySelector("tr[onclick*=\\"'" + id + "'\\"]")?.querySelector('.exp-btn');
  if (btn) btn.textContent = open ? '▶ 详情' : '▼ 详情';
}}
function toggleEl(id) {{
  var el = document.getElementById(id);
  if (!el) return;
  el.style.display = el.style.display === 'none' ? '' : 'none';
}}
</script>
</head>
<body>
<div class="rpt-header">
  <h1>Validate 变异重放验证报告</h1>
  {f'<span class="sub">来源: {src_label}</span>' if src_label else ""}
</div>
<div class="rpt-main">

<div class="stats-row">
  <div class="stat-card"><div class="stat-val">{len(fields_requested)}</div><div class="stat-label">验证字段</div></div>
  <div class="stat-card"><div class="stat-val">{n_total}</div><div class="stat-label">变异记录总数</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--ok)">{n_ok}</div><div class="stat-label">成功重放</div></div>
  <div class="stat-card"><div class="stat-val" style="color:#7c3aed">{n_corrected}</div><div class="stat-label">修正成功</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--warn)">{n_unhealthy}</div><div class="stat-label">修正失败</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--err)">{n_err}</div><div class="stat-label">重放失败</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--tx2)">{n_skip}</div><div class="stat-label">跳过</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--acc)">{n_rel}</div><div class="stat-label">新增关联分支</div></div>
</div>

{missing_html}

<h2>变异重放明细</h2>
<p class="muted" style="margin-bottom:10px">
  点击任意行展开：CR 变更 diff、采集器数据前后对比（含表达式/变量取值）、原始 JSON
</p>
<table class="main-tbl">
  <thead>
    <tr>
      <th>字段路径</th><th>变异类型</th><th>状态</th>
      <th>字段值变化</th><th>Branch diff</th><th>关联</th><th>关联 Branch</th>
    </tr>
  </thead>
  <tbody>{rows_html}
  </tbody>
</table>
</div>
</body></html>"""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"validate 报告已生成: {output_path}")