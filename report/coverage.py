import logging
import os
from datetime import datetime
from typing import List

from report.style import full_report_css

logger = logging.getLogger(__name__)


def generate_coverage_test_report(
    results: List[dict],
    targets: List[dict],
    branch_meta_index: dict,
    output_path: str,
) -> None:
    """Generate a self-contained HTML report for coverage-test results."""
    import difflib
    from html import escape as _esc

    def _yaml_diff_html(before: str, after: str) -> str:
        if not before and not after:
            return ""
        before_lines = (before or "").splitlines(keepends=True)
        after_lines = (after or "").splitlines(keepends=True)
        parts = []
        for line in difflib.unified_diff(
            before_lines, after_lines, fromfile="baseline CR", tofile="mutated CR", n=3
        ):
            esc = _esc(line.rstrip("\n"))
            if line.startswith("+++") or line.startswith("---"):
                parts.append(f'<span class="dh">{esc}</span>\n')
            elif line.startswith("@@"):
                parts.append(f'<span class="dc">{esc}</span>\n')
            elif line.startswith("+"):
                parts.append(f'<span class="da">{esc}</span>\n')
            elif line.startswith("-"):
                parts.append(f'<span class="dr">{esc}</span>\n')
            else:
                parts.append(f'<span class="dx">{esc}</span>\n')
        return '<pre class="diff-pre">' + "".join(parts) + "</pre>"

    total = len(results)
    success = sum(1 for r in results if r["success"])
    fail = total - success


    cards_html = ""
    for r in results:
        bi = r["branch_index"]
        tgt_val = r["target_value"]
        ok = r["success"]
        status_cls = "ct-ok" if ok else "ct-fail"
        status_lbl = "✓ COVERED" if ok else "✗ NOT COVERED"
        cond_esc = _esc(r["condition"][:120])
        func_esc = _esc(r["func"])
        file_esc = _esc(f"{r['file']}:{r['line']}")
        bv = r["baseline_value"]
        av = r["achieved_value"]


        cr_diff_html = _yaml_diff_html(r["baseline_cr_yaml"], r["mutated_cr_yaml"])


        cr_fields_html = ""
        if r["cr_changed_fields"]:
            rows = "".join(
                f'<tr><td class="mono">{_esc(f)}</td></tr>'
                for f in r["cr_changed_fields"]
            )
            cr_fields_html = (
                f'<table class="ct-tbl"><thead><tr><th>Changed CR Field</th></tr></thead>'
                f"<tbody>{rows}</tbody></table>"
            )
        else:
            cr_fields_html = '<span class="muted">No CR fields changed</span>'


        changed_brs = r.get("changed_branches", [])[:20]
        br_rows = ""
        for rec in changed_brs:
            cb_bi = rec.get("branch_index", "?")
            cb_bv = rec.get("before_value")
            cb_av = rec.get("after_value")
            cb_bm = branch_meta_index.get(cb_bi, {})
            cb_cnd = _esc((cb_bm.get("Fmt") or cb_bm.get("Raw") or "")[:60])
            br_rows += (
                f'<tr><td class="mono">[{cb_bi}]</td>'
                f'<td class="mono ct-cond">{cb_cnd}</td>'
                f'<td><span class="bv-before">{cb_bv}</span>'
                f' → <span class="bv-after">{cb_av}</span></td></tr>'
            )
        br_table = ""
        if br_rows:
            br_table = (
                f'<table class="ct-tbl"><thead><tr>'
                f"<th>#</th><th>Condition</th><th>Value Change</th>"
                f"</tr></thead><tbody>{br_rows}</tbody></table>"
            )
        else:
            br_table = '<span class="muted">No branch value changes recorded</span>'


        rel_html = ""
        if r["related_fields"]:
            rel_html = " ".join(
                f'<span class="rel-tag">{_esc(fp)}</span>'
                for fp in r["related_fields"][:10]
            )
        else:
            rel_html = '<span class="muted">None known</span>'

        error_html = ""
        if r["error"]:
            error_html = f'<div class="ct-err">⚠ {_esc(r["error"])}</div>'


        attempt_logs = r.get("attempt_logs", [])

        def _parse_prompt_sections(prompt_text: str) -> list:
            """Split a prompt into (heading, body) sections by '## ' markers."""
            sections = []
            current_head = "Preamble"
            current_lines: list = []
            for line in prompt_text.splitlines():
                if line.startswith("## "):
                    if current_lines:
                        sections.append(
                            (current_head, "\n".join(current_lines).strip())
                        )
                    current_head = line[3:].strip()
                    current_lines = []
                else:
                    current_lines.append(line)
            if current_lines:
                sections.append((current_head, "\n".join(current_lines).strip()))
            return sections

        attempts_html = ""
        for alog in attempt_logs:
            a_num = alog.get("attempt", "?")
            a_llm = alog.get("llm_sec")
            a_apply = alog.get("apply_sec")
            a_total = alog.get("total_sec")
            a_out = _esc(alog.get("outcome", ""))
            a_out_cls = "ok-mark" if alog.get("outcome") == "success" else "fail-mark"

            timing_parts = []
            if a_llm is not None:
                timing_parts.append(f"LLM: <b>{a_llm}s</b>")
            if a_apply is not None:
                timing_parts.append(f"apply+collect: <b>{a_apply}s</b>")
            if a_total is not None:
                timing_parts.append(f"total: <b>{a_total}s</b>")
            timing_html = " &nbsp;·&nbsp; ".join(timing_parts)


            prompt_text = alog.get("prompt", "")
            sections = _parse_prompt_sections(prompt_text)
            sections_html = ""
            for s_head, s_body in sections:
                if not s_body.strip():
                    continue
                sections_html += (
                    f'<div class="ps-section">'
                    f'<div class="ps-head">{_esc(s_head)}</div>'
                    f'<pre class="ps-body">{_esc(s_body)}</pre>'
                    f"</div>"
                )


            response_html = ""
            if alog.get("response"):
                response_html = (
                    f'<div class="ps-section">'
                    f'<div class="ps-head" style="color:var(--yellow)">LLM Response</div>'
                    f'<pre class="ps-body" style="color:var(--yellow)">{_esc(alog["response"])}</pre>'
                    f"</div>"
                )

            attempts_html += f"""
<details class="attempt-det">
  <summary>
    Attempt {a_num}
    &nbsp;<span class="{a_out_cls}" style="font-size:11px">{a_out}</span>
    &nbsp;<span class="muted" style="font-size:11px">{timing_html}</span>
  </summary>
  <div class="prompt-viewer">
    {sections_html}
    {response_html}
  </div>
</details>"""

        attempt_panel = ""
        if attempts_html:
            attempt_panel = f"""
    <details>
      <summary>Attempts &amp; Prompts ({len(attempt_logs)})</summary>
      {attempts_html}
    </details>"""


        tgt_llm_sec = round(sum((a.get("llm_sec") or 0) for a in attempt_logs), 2)
        tgt_apply_sec = round(sum((a.get("apply_sec") or 0) for a in attempt_logs), 2)
        tgt_total_sec = round(sum((a.get("total_sec") or 0) for a in attempt_logs), 2)
        timing_row = ""
        if attempt_logs:
            timing_row = (
                f'<div class="ct-timing">'
                f'<span class="muted">⏱ attempts: <b>{len(attempt_logs)}</b></span>'
                f' &nbsp;·&nbsp; <span class="muted">LLM: <b>{tgt_llm_sec}s</b></span>'
                f' &nbsp;·&nbsp; <span class="muted">apply+collect: <b>{tgt_apply_sec}s</b></span>'
                f' &nbsp;·&nbsp; <span class="muted">total: <b>{tgt_total_sec}s</b></span>'
                f"</div>"
            )

        cards_html += f"""
<div class="ct-card {status_cls}">
  <div class="ct-header">
    <span class="ct-badge {status_cls}">{status_lbl}</span>
    <span class="ct-bi">branch[{bi}]</span>
    <span class="ct-target">target = <b>{"True" if tgt_val else "False"}</b></span>
    <span class="ct-cond mono">{cond_esc}</span>
  </div>
  <div class="ct-meta">
    <span class="muted">func:</span> <span class="mono">{func_esc}</span>
    &nbsp;·&nbsp;
    <span class="muted">loc:</span> <span class="mono">{file_esc}</span>
    &nbsp;·&nbsp;
    <span class="muted">baseline→achieved:</span>
    <span class="bv-before">{bv}</span> → <span class="bv-after">{av}</span>
  </div>
  {timing_row}
  {error_html}
  <div class="ct-sections">
    <details open>
      <summary>Related CR Fields ({len(r["related_fields"])})</summary>
      <div class="ct-rel">{rel_html}</div>
    </details>
    <details open>
      <summary>CR Diff (baseline → mutated)</summary>
      {cr_diff_html if cr_diff_html else '<span class="muted">No CR changes</span>'}
    </details>
    <details>
      <summary>Changed CR Fields ({len(r["cr_changed_fields"])})</summary>
      {cr_fields_html}
    </details>
    <details>
      <summary>Branch Value Changes ({len(changed_brs)} shown)</summary>
      {br_table}
    </details>
    {attempt_panel}
  </div>
</div>"""

    _css = full_report_css()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GSOD 覆盖测试报告</title>
<style>
{_css}
.ct-card{{background:var(--card);border:1px solid var(--bdr);border-radius:var(--rs);
  margin-bottom:16px;overflow:hidden}}
.ct-card.ct-ok{{border-left:3px solid var(--ok)}}
.ct-card.ct-fail{{border-left:3px solid var(--err)}}
.ct-header{{display:flex;align-items:baseline;gap:10px;padding:12px 16px 8px;
  flex-wrap:wrap;border-bottom:1px solid var(--bdr)}}
.ct-badge{{font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px}}
.ct-ok .ct-badge{{background:var(--ok-l);color:var(--ok)}}
.ct-fail .ct-badge{{background:var(--err-l);color:var(--err)}}
.ct-bi{{font-size:12px;font-weight:600;color:var(--acc)}}
.ct-target{{font-size:12px;color:var(--warn)}}
.ct-cond{{font-size:12px;color:var(--tx);flex:1}}
.ct-meta{{padding:6px 16px;font-size:11px;color:var(--tx2);border-bottom:1px solid var(--bdr)}}
.ct-err{{margin:8px 16px;padding:6px 10px;background:var(--err-l);color:var(--err);border-radius:4px;font-size:11px}}
.ct-sections{{padding:12px 16px;display:flex;flex-direction:column;gap:10px}}
.ct-rel{{display:flex;flex-wrap:wrap;gap:6px;padding:6px 0}}
.rel-tag{{background:var(--acc-l);color:var(--acc);font-size:11px;padding:2px 8px;border-radius:4px;font-family:monospace}}
.ct-tbl{{width:100%;border-collapse:collapse;font-size:11px;margin-top:6px}}
.ct-tbl th{{background:rgba(79,70,229,.04);color:var(--tx2);padding:4px 8px;text-align:left;border-bottom:1px solid var(--bdr)}}
.ct-tbl td{{padding:3px 8px;border-bottom:1px solid rgba(200,210,225,.35)}}
.bv-before{{color:var(--err)}}.bv-after{{color:var(--ok)}}
.ok-mark{{color:var(--ok);font-weight:700}}.fail-mark{{color:var(--err);font-weight:700}}
.ct-timing{{padding:4px 14px;font-size:11px;border-top:1px solid var(--bdr);background:#f8fafc;color:var(--tx2)}}
.attempt-det{{border:1px solid var(--bdr);border-radius:var(--rs);margin-top:6px;background:#f8fafc}}
.attempt-det>summary{{padding:6px 10px;font-size:12px;font-weight:600;cursor:pointer;user-select:none;color:var(--tx2)}}
.attempt-det>summary:hover{{color:var(--tx)}}
.attempt-det[open]>summary{{color:var(--tx);border-bottom:1px solid var(--bdr)}}
.prompt-viewer{{display:flex;flex-direction:column;gap:0}}
.ps-section{{border-bottom:1px solid var(--bdr)}}.ps-section:last-child{{border-bottom:none}}
.ps-head{{background:#f1f5f9;padding:4px 12px;font-size:11px;font-weight:700;color:var(--acc);letter-spacing:.04em;text-transform:uppercase}}
.ps-body{{background:#f8fafc;padding:8px 12px;font-size:11px;line-height:1.55;font-family:'Cascadia Code',Consolas,monospace;color:var(--tx);white-space:pre-wrap;word-break:break-word;margin:0;max-height:420px;overflow-y:auto}}
</style>
</head>
<body>
<div class="rpt-header">
  <h1>GSOD 覆盖测试报告</h1>
  <span class="sub">生成于 {ts} · {total} 个目标</span>
</div>
<div class="rpt-main">
<div class="stats-row">
  <div class="stat-card"><div class="stat-val" style="color:var(--ok)">{success}</div><div class="stat-label">已覆盖</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--err)">{fail}</div><div class="stat-label">未覆盖</div></div>
  <div class="stat-card"><div class="stat-val">{total}</div><div class="stat-label">总目标数</div></div>
  <div class="stat-card"><div class="stat-val">{round(100 * success / max(total, 1))}%</div>
    <div class="stat-label">覆盖率</div></div>
</div>
{cards_html}
</div>
</body>
</html>"""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"[coverage-test] 报告已生成: {output_path}")