import json
import logging
import os
from html import escape as _html_esc
from typing import Optional

from report.style import full_report_css

logger = logging.getLogger(__name__)


def _fmt_bool(v) -> str:
    if v is True or str(v).lower() in ("true", "1"):
        return '<span class="v-true">True</span>'
    if v is False or str(v).lower() in ("false", "0"):
        return '<span class="v-false">False</span>'
    return f'<span class="v-none">{_html_esc(str(v))}</span>'


def _fmt_val(v) -> str:
    return _html_esc(str(v)) if v is not None else '<span class="v-none">—</span>'


def _render_variables(vars_dict: dict) -> str:
    """render variables sub-table from ExpressionData.variables"""
    if not vars_dict:
        return '<span class="v-none">（无变量）</span>'
    rows = ""
    for vi in vars_dict.values() if isinstance(vars_dict, dict) else vars_dict:
        if isinstance(vi, dict):
            idx = vi.get("variable_index", vi.get("id", "?"))
            val = _html_esc(str(vi.get("value", "?")))
            typ = _html_esc(str(vi.get("type", "")))
            kind = _html_esc(str(vi.get("kind", "")))
            rows += (
                f"<tr><td class='vi-idx'>[{idx}]</td><td class='vi-kind'>{kind}</td>"
                f"<td class='vi-type'>{typ}</td><td class='vi-val'>{val}</td></tr>"
            )
    return (
        f'<table class="var-tbl">{rows}</table>'
        if rows
        else '<span class="v-none">（无变量）</span>'
    )


def _render_expressions(exprs, label_cls: str = "") -> str:
    """render expressions list/dict from BranchData.expressions"""
    if not exprs:
        return '<span class="v-none">（无表达式数据）</span>'
    items = exprs.values() if isinstance(exprs, dict) else exprs
    out = ""
    for e in items:
        if not isinstance(e, dict):
            continue
        eidx = e.get("expression_index", e.get("id", "?"))
        eval_ = _html_esc(str(e.get("value", "?")))
        etype = _html_esc(str(e.get("type", "")))
        hit = e.get("hit_case_index", "")
        out += (
            f'<div class="expr-block {label_cls}">'
            f'  <span class="expr-hdr">expr[{eidx}] = <b>{eval_}</b>'
            f'  <span class="expr-meta">type={etype} hit={hit}</span></span>'
            f"  {_render_variables(e.get('variables', {}))}"
            f"</div>"
        )
    return out if out else '<span class="v-none">（无表达式数据）</span>'


def _cr_yaml_diff_html(before_yaml: str, after_yaml: str) -> str:
    """Return an HTML <pre> block showing a unified diff between two CR YAMLs."""
    import difflib as _dl

    if not before_yaml or not after_yaml:
        fallback = after_yaml or before_yaml or ""
        return f'<pre class="det-pre">{_html_esc(fallback)}</pre>'
    before_lines = before_yaml.splitlines(keepends=True)
    after_lines = after_yaml.splitlines(keepends=True)
    diff_lines = list(
        _dl.unified_diff(
            before_lines,
            after_lines,
            fromfile="before (baseline CR)",
            tofile="after (mutated CR)",
            lineterm="",
        )
    )
    if not diff_lines:
        return '<p class="v-none">CR 无变化（diff 为空）</p>'
    parts = []
    for line in diff_lines:
        line_esc = _html_esc(line.rstrip("\n"))
        if line.startswith("+++") or line.startswith("---") or line.startswith("@@"):
            parts.append(f'<span class="diff-hdr">{line_esc}</span>')
        elif line.startswith("+"):
            parts.append(f'<span class="diff-add">{line_esc}</span>')
        elif line.startswith("-"):
            parts.append(f'<span class="diff-rem">{line_esc}</span>')
        else:
            parts.append(f'<span class="diff-ctx">{line_esc}</span>')
    return f'<pre class="det-pre diff-pre">{chr(10).join(parts)}</pre>'


def _branch_compare_table(
    after_instr: dict,
    diff_raw: dict,
    baseline_traces: dict,
    branch_meta_index: Optional[dict],
) -> str:
    """Build a rich before/after comparison table across all branches."""
    after_traces: dict = {
        t["branch_index"]: t
        for t in after_instr.get("traces", [])
        if isinstance(t, dict)
    }
    changed_bis = {r["branch_index"]: r for r in diff_raw.get("changed", [])}
    added_bis = set(diff_raw.get("added", []))
    removed_bis = set(diff_raw.get("removed", []))

    all_bis = sorted(set(list(baseline_traces.keys()) + list(after_traces.keys())))
    if not all_bis:
        return '<p class="v-none">没有可对比的 branch 数据</p>'

    rows = ""
    for bi in all_bis:
        bt = baseline_traces.get(bi)
        at = after_traces.get(bi)
        bm = (branch_meta_index or {}).get(bi, {})
        cond = _html_esc((bm.get("Fmt") or bm.get("Raw") or "")[:80])
        func = _html_esc(bm.get("Func", ""))
        level = bm.get("CallLevel", "?")

        if bi in added_bis:
            row_cls, change_badge = "br-added", '<span class="badge-add">＋ 新增</span>'
        elif bi in removed_bis:
            row_cls, change_badge = (
                "br-removed",
                '<span class="badge-rem">－ 消失</span>',
            )
        elif bi in changed_bis:
            row_cls, change_badge = (
                "br-changed",
                '<span class="badge-chg">⇄ 变化</span>',
            )
        else:
            row_cls, change_badge = "br-same", '<span class="badge-same">＝ 不变</span>'

        bv_html = _fmt_bool(bt["value"]) if bt else '<span class="v-none">—</span>'
        av_html = _fmt_bool(at["value"]) if at else '<span class="v-none">—</span>'

        has_exprs = bt or at
        exp_id = f"br-{bi}"
        exp_btn = (
            f'<button class="exp-btn" onclick="toggleEl(\'{exp_id}\')">▶ 表达式/变量</button>'
            if has_exprs
            else ""
        )
        meta_html = (
            f'<div class="br-meta"><code>{cond}</code> <span class="br-func">{func} (level={level})</span></div>'
            if cond
            else ""
        )
        b_exprs_html = _render_expressions(
            bt.get("expressions", {}) if bt else {}, "expr-before"
        )
        a_exprs_html = _render_expressions(
            at.get("expressions", {}) if at else {}, "expr-after"
        )
        expr_panel = (
            f'<tr id="{exp_id}" style="display:none" class="expr-row">'
            f'<td colspan="6">'
            f'<div class="expr-cmp">'
            f'  <div class="expr-col"><div class="expr-col-hdr">变更前 (baseline)</div>{b_exprs_html}</div>'
            f'  <div class="expr-col"><div class="expr-col-hdr">变更后 (after)</div>{a_exprs_html}</div>'
            f"</div></td></tr>"
        )
        rows += f"""
        <tr class="br-row {row_cls}" onclick="toggleEl('{exp_id}')" style="cursor:pointer">
          <td class="bi-cell">b[{bi}]</td>
          <td>{change_badge}</td>
          <td>{bv_html}</td>
          <td>{av_html}</td>
          <td class="cond-cell">{meta_html}{exp_btn}</td>
        </tr>
        {expr_panel}"""

    return f"""
        <table class="br-tbl">
          <thead><tr>
            <th>Branch</th><th>变化</th>
            <th>变更前值</th><th>变更后值</th>
            <th>条件 / 表达式 / 变量</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>"""


def _build_detail_panel_html(
    m: dict,
    row_id: str,
    baseline_instr: dict,
    baseline_traces: dict,
    branch_meta_index: Optional[dict],
) -> str:
    """Build the expandable detail panel HTML for a single mutation_log entry."""
    status = m.get("status", "?")
    parts = []

    if status != "ok":
        err = _html_esc(str(m.get("error", "(no error message)")))
        parts.append(
            f'<div class="det-section"><div class="det-title">错误信息</div>'
            f'<pre class="det-pre err-pre">{err}</pre></div>'
        )

    cr_chg = m.get("cr_changed_fields", [])
    targeted = m.get("field", "")
    if cr_chg:
        chg_items = "".join(
            f'<span class="cr-chg-field{" cr-chg-extra" if fp2 != targeted else ""}">'
            f"{_html_esc(fp2)}</span>"
            for fp2 in cr_chg
        )
        note = (
            " <span class='v-none'>(★ = 目标字段, 灰色 = 额外改变)</span>"
            if any(fp2 != targeted for fp2 in cr_chg)
            else ""
        )
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title">实际变化的 CR 字段{note}</div>'
            f'<div class="cr-chg-list">{chg_items}</div></div>'
        )

    sub_muts = m.get("sub_mutations", [])
    if len(sub_muts) > 1:
        sm_rows = ""
        for sm in sub_muts:
            sm_st = sm.get("status", "?")
            sm_cls = "sm-ok" if sm_st == "ok" else "sm-fail"
            sm_ds = sm.get("diff_summary", {})
            sm_chg = sm.get("cr_changed_fields", [])
            if sm_st == "ok":
                nc, na, nr = (
                    sm_ds.get("changed", 0),
                    sm_ds.get("added", 0),
                    sm_ds.get("removed", 0),
                )
                sm_ds_html = "".join(
                    [
                        f'<span class="badge-chg">⇄{nc}</span> ' if nc else "",
                        f'<span class="badge-add">＋{na}</span> ' if na else "",
                        f'<span class="badge-rem">－{nr}</span>' if nr else "",
                    ]
                )
            else:
                sm_ds_html = f'<span class="err-txt">{_html_esc(sm.get("error", "")[:80])}</span>'
            sm_cf_html = (
                " ".join(
                    f'<span class="cr-chg-field{" cr-chg-extra" if fp2 != targeted else ""}">'
                    f"{_html_esc(fp2)}</span>"
                    for fp2 in sm_chg
                )
                if sm_chg
                else '<span class="v-none">—</span>'
            )
            sm_rows += (
                f'<tr class="{sm_cls}"><td class="sm-kind">{_html_esc(sm.get("kind", "?"))}</td>'
                f'<td>{sm_ds_html}</td><td class="sm-fields">{sm_cf_html}</td></tr>'
            )
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title">子变异明细 (sub_mutations)</div>'
            f'<table class="sm-tbl"><thead><tr><th>类型</th><th>Branch diff</th>'
            f"<th>实际改变字段</th></tr></thead><tbody>{sm_rows}</tbody></table></div>"
        )

    base_yaml = m.get("base_cr_yaml", "")
    cr_yaml = m.get("mutated_cr_yaml", "")
    if cr_yaml:
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title">CR 变更 diff（变异前 → 变异后）</div>'
            f"{_cr_yaml_diff_html(base_yaml, cr_yaml)}</div>"
        )

    sub_muts_with_base = [
        sm
        for sm in m.get("sub_mutations", [])
        if sm.get("base_cr_yaml") and sm.get("mutated_cr_yaml")
    ]
    if len(sub_muts_with_base) > 1:
        sub_diff_parts = []
        for sm in sub_muts_with_base:
            sm_kind = _html_esc(sm.get("kind", "?"))
            sm_st = sm.get("status", "?")
            sm_hdr_cls = "sm-ok" if sm_st == "ok" else "sm-fail"
            sub_diff_parts.append(
                f'<div class="sm-diff-block">'
                f'<div class="sm-diff-hdr {sm_hdr_cls}">{sm_kind} ({sm_st})</div>'
                f"{_cr_yaml_diff_html(sm.get('base_cr_yaml', ''), sm.get('mutated_cr_yaml', ''))}"
                f"</div>"
            )
        sdp_id = f"subdiff-{row_id}"
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title" style="cursor:pointer" onclick="toggleEl(\'{sdp_id}\')">'
            f"▶ 各子变异 CR diff 明细</div>"
            f'<div id="{sdp_id}" style="display:none">{"".join(sub_diff_parts)}</div></div>'
        )

    after_instr = m.get("after_instr")
    diff_raw = m.get("diff_raw") or {}
    if after_instr:
        cmp_table = _branch_compare_table(
            after_instr, diff_raw, baseline_traces, branch_meta_index
        )
        parts.append(
            '<div class="det-section">'
            '<div class="det-title">采集器数据对比（变更前 baseline vs 变更后 after）</div>'
            '<p class="det-hint">点击 branch 行展开表达式/变量详情；黄色=值变化，绿色=新增，红色=消失</p>'
            + cmp_table
            + "</div>"
        )

    if after_instr or baseline_instr:
        raw_id = f"raw-{row_id}"
        b_json = _html_esc(json.dumps(baseline_instr, ensure_ascii=False, indent=2))
        a_json = _html_esc(json.dumps(after_instr or {}, ensure_ascii=False, indent=2))
        parts.append(
            f'<div class="det-section">'
            f'<div class="det-title" style="cursor:pointer" onclick="toggleEl(\'{raw_id}\')">'
            f"▶ 原始 JSON 数据（调试用）</div>"
            f'<div id="{raw_id}" style="display:none">'
            f'<div class="expr-cmp">'
            f'  <div class="expr-col"><div class="expr-col-hdr">baseline_instr (JSON)</div>'
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


def generate_exploration_report(
    ckpt: dict,
    output_path: str,
    branch_meta_index: Optional[dict] = None,
):
    """从 checkpoint 生成 explore-all 关联分析 HTML 报告。

    detail panel 展示:
      1. 变异后 CR (YAML)
      2. 采集器数据 Before/After 对比表 —— 逐分支展示条件/表达式/变量及值变化
      3. 原始 JSON (baseline_instr / after_instr) 可折叠调试用
    """
    ea = ckpt.get("explore_all", {})
    field_relations = ckpt.get("field_relations", {})
    baseline_instr = ea.get("baseline_instr") or {}


    baseline_traces: dict = {
        t["branch_index"]: t
        for t in baseline_instr.get("traces", [])
        if isinstance(t, dict)
    }

    mutation_log = ea.get("mutation_log", [])
    total = len(mutation_log)
    n_ok = sum(1 for m in mutation_log if m["status"] == "ok")
    n_fail = total - n_ok
    n_rel = len(field_relations)
    n_with_rel = sum(
        1 for m in mutation_log if m["status"] == "ok" and m["field"] in field_relations
    )


    rows_html = ""
    for idx, m in enumerate(mutation_log):
        fp = _html_esc(m.get("field", ""))
        ftype = _html_esc(m.get("field_type", ""))
        depth = m.get("field_depth", "")
        status = m.get("status", "?")
        s_cls = "ea-ok" if status == "ok" else "ea-fail"
        ds = m.get("diff_summary", {})
        has_rel = fp in field_relations
        row_id = str(idx)

        if status == "ok":
            n_chg = ds.get("changed", 0)
            n_add = ds.get("added", 0)
            n_rem = ds.get("removed", 0)
            parts_d = []
            if n_chg:
                parts_d.append(f'<span class="badge-chg">⇄{n_chg}</span>')
            if n_add:
                parts_d.append(f'<span class="badge-add">＋{n_add}</span>')
            if n_rem:
                parts_d.append(f'<span class="badge-rem">－{n_rem}</span>')
            detail = (
                " ".join(parts_d) if parts_d else '<span class="v-none">无 diff</span>'
            )
        else:
            detail = f'<span class="err-txt">{_html_esc(str(m.get("error", ""))[:120])}</span>'

        rel_badge = (
            '<span class="rel-yes">✓ 关联</span>'
            if has_rel
            else '<span class="rel-no">— 无关联</span>'
        )

        branches_html = ""
        if has_rel:
            fdata = field_relations[fp]
            bis = fdata.get("branch_indices") or []
            vm = fdata.get("variable_mappings", {})
            ef = fdata.get("expression_fmts", {})
            bi_parts = []
            for b in sorted(bis)[:12]:
                bi_key = str(b)
                bm_info = (branch_meta_index or {}).get(b, {})
                cond_short = (bm_info.get("Fmt") or bm_info.get("Raw") or "")[:50]

                v_fmts = [
                    vinfo.get("variable_fmt", "")
                    for vinfo in vm.get(bi_key, {}).values()
                    if vinfo.get("variable_fmt")
                ]
                if not v_fmts:
                    v_fmts = ef.get(bi_key, [])
                v_html = ""
                if v_fmts:
                    v_html = " ".join(
                        f'<span class="var-tag">{_html_esc(vf[:40])}</span>'
                        for vf in v_fmts[:3]
                    )
                cond_tip = f' title="{_html_esc(cond_short)}"' if cond_short else ""
                bi_parts.append(f'<span class="bi-tag"{cond_tip}>b[{b}]</span>{v_html}')
            branches_html = " ".join(bi_parts)
            if len(bis) > 12:
                branches_html += f'<span class="v-none"> +{len(bis) - 12}</span>'

        expand_btn = f'<button class="exp-btn" onclick="event.stopPropagation();toggleDet(\'{row_id}\')">▶ 详情</button>'

        rows_html += f"""
      <tr class="{s_cls}" onclick="toggleDet('{row_id}')" style="cursor:pointer">
        <td class="fp">{fp} {expand_btn}</td>
        <td>{ftype}</td>
        <td class="depth">{depth}</td>
        <td>{'<span class="ok-mark">✓</span>' if status == "ok" else '<span class="fail-mark">✗</span>'}</td>
        <td>{detail}</td>
        <td>{rel_badge}</td>
        <td class="branches">{branches_html}</td>
      </tr>"""
        rows_html += _build_detail_panel_html(
            m, row_id, baseline_instr, baseline_traces, branch_meta_index
        )


    _css = full_report_css()
    html = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Explore-All 关联分析报告</title>
<style>
{_css}
  /* main table */
  table.main-tbl{{width:100%;border-collapse:collapse;font-size:13px;}}
  table.main-tbl th{{background:rgba(79,70,229,.04);color:var(--tx2);padding:8px 10px;text-align:left;
      font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;
      border-bottom:2px solid var(--bdr);position:sticky;top:0;z-index:2;}}
  table.main-tbl td{{padding:6px 10px;border-bottom:1px solid rgba(200,210,225,.35);vertical-align:middle;}}
  tr.ea-ok:hover > td{{background:rgba(79,70,229,.03);}}
  tr.ea-fail > td{{color:var(--tx3);}}
  tr.ea-fail:hover > td{{background:rgba(220,38,38,.04);}}
  .fp{{color:var(--acc);word-break:break-all;font-family:'Cascadia Code',Consolas,monospace;font-size:12px;}}
  .depth{{color:var(--tx2);text-align:center;}}
  .branches{{line-height:1.8;}}
  .bi-tag{{background:#e0e7ff;border:1px solid rgba(79,70,229,.2);border-radius:4px;
           padding:1px 6px;margin:1px;font-size:11px;color:var(--acc);}}
  .rel-yes{{color:var(--ok);font-weight:bold;}}
  .rel-no{{color:var(--tx3);}}
  .ok-mark{{color:var(--ok);font-weight:bold;font-size:1.1em;}}
  .fail-mark{{color:var(--err);font-weight:bold;font-size:1.1em;}}
  .err-txt{{color:var(--err);font-size:12px;}}
  /* detail panel */
  .det-row > td{{padding:0;border-bottom:2px solid rgba(79,70,229,.15);}}
  .det-wrap{{background:#f8fafc;padding:16px 20px;border-left:3px solid var(--acc);}}
  .det-section{{margin-bottom:16px;}}
  .det-title{{color:var(--acc);font-weight:bold;font-size:13px;margin-bottom:6px;padding:4px 0;border-bottom:1px solid var(--bdr);}}
  .det-hint{{color:var(--tx2);font-size:11px;margin:4px 0 8px;}}
  .det-pre{{background:#fff;border:1px solid var(--bdr);border-radius:6px;
            padding:10px;overflow-x:auto;white-space:pre;font-size:12px;
            color:var(--tx);margin:0;max-height:350px;overflow-y:auto;font-family:'Cascadia Code',Consolas,monospace;}}
  .err-pre{{border-color:rgba(220,38,38,.3);color:var(--err);}}
  /* branch comparison table */
  table.br-tbl{{width:100%;border-collapse:collapse;font-size:12px;margin-top:4px;}}
  table.br-tbl th{{background:rgba(79,70,229,.04);color:var(--tx2);padding:5px 8px;text-align:left;border-bottom:1px solid var(--bdr);}}
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
  /* badge */
  .badge-chg{{color:var(--warn);background:#fef3c7;border:1px solid rgba(217,119,6,.2);border-radius:4px;padding:1px 6px;font-size:11px;}}
  .badge-add{{color:var(--ok);background:#d1fae5;border:1px solid rgba(5,150,105,.2);border-radius:4px;padding:1px 6px;font-size:11px;}}
  .badge-rem{{color:var(--err);background:#fee2e2;border:1px solid rgba(220,38,38,.2);border-radius:4px;padding:1px 6px;font-size:11px;}}
  .badge-same{{color:var(--tx3);font-size:11px;}}
  /* value display */
  .v-true{{color:var(--ok);font-weight:bold;}}
  .v-false{{color:var(--err);font-weight:bold;}}
  .v-none{{color:var(--tx3);font-style:italic;}}
  /* expression/variable display */
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
  .vi-idx{{color:var(--tx2);width:30px;}}
  .vi-kind{{color:#7c3aed;width:80px;}}
  .vi-type{{color:var(--tx2);width:100px;}}
  .vi-val{{color:var(--warn);font-weight:bold;}}
  /* buttons */
  .exp-btn{{background:none;border:1px solid var(--bdr);color:var(--tx2);border-radius:4px;
            padding:1px 8px;font-size:11px;cursor:pointer;margin-left:6px;vertical-align:middle;}}
  .exp-btn:hover{{background:rgba(79,70,229,.06);color:var(--tx);}}
  /* variable fmts inline with branch tags */
  .var-tag{{background:#ede9fe;border:1px solid rgba(124,58,237,.2);border-radius:4px;
            padding:1px 6px;margin:1px;font-size:11px;color:#7c3aed;display:inline-block;}}
  /* CR changed fields */
  .cr-chg-list{{display:flex;flex-wrap:wrap;gap:5px;padding:4px 0;}}
  .cr-chg-field{{background:#e0e7ff;border:1px solid rgba(79,70,229,.2);border-radius:4px;
                 padding:2px 8px;font-size:12px;color:var(--acc);}}
  .cr-chg-extra{{background:#fef3c7;border-color:rgba(217,119,6,.2);color:var(--warn);}}
  /* sub-mutations table */
  table.sm-tbl{{width:100%;border-collapse:collapse;font-size:12px;margin-top:4px;}}
  table.sm-tbl th{{background:rgba(79,70,229,.04);color:var(--tx2);padding:4px 8px;text-align:left;border-bottom:1px solid var(--bdr);}}
  table.sm-tbl td{{padding:4px 8px;border-bottom:1px solid rgba(200,210,225,.35);vertical-align:top;}}
  tr.sm-ok > td{{background:#d1fae5;}}
  tr.sm-fail > td{{background:#fee2e2;color:var(--tx3);}}
  .sm-kind{{color:var(--warn);font-weight:bold;width:90px;white-space:nowrap;}}
  .sm-fields{{max-width:500px;line-height:1.8;}}
  /* CR YAML diff */
  .diff-pre{{font-size:12px;line-height:1.4;max-height:400px;overflow-y:auto;}}
  .diff-add{{display:block;background:#d1fae5;color:var(--ok);}}
  .diff-rem{{display:block;background:#fee2e2;color:var(--err);}}
  .diff-hdr{{display:block;color:var(--tx2);background:#f1f5f9;}}
  .diff-ctx{{display:block;color:var(--tx3);}}
  /* per-sub diff blocks */
  .sm-diff-block{{margin-bottom:10px;}}
  .sm-diff-hdr{{font-size:12px;font-weight:bold;padding:3px 8px;border-radius:6px 6px 0 0;}}
  .sm-diff-hdr.sm-ok{{background:#d1fae5;color:var(--ok);}}
  .sm-diff-hdr.sm-fail{{background:#fee2e2;color:var(--err);}}
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
  <h1>Explore-All 关联分析报告</h1>
  <span class="sub">checkpoint 创建: {ckpt.get("created_at", "")[:19]}</span>
</div>
<div class="rpt-main">

<div class="stats-row">
  <div class="stat-card"><div class="stat-val">{total}</div><div class="stat-label">已探索字段</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--ok)">{n_ok}</div><div class="stat-label">成功变异</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--err)">{n_fail}</div><div class="stat-label">变异失败</div></div>
  <div class="stat-card"><div class="stat-val">{n_rel}</div><div class="stat-label">关联字段数</div></div>
  <div class="stat-card"><div class="stat-val" style="color:var(--warn)">{n_with_rel}</div><div class="stat-label">字段有 branch 映射</div></div>
</div>

<h2>字段探索明细</h2>
<p class="muted" style="margin-bottom:10px">点击任意行展开详情：变异 CR、采集器数据前后对比（含表达式/变量）、原始 JSON</p>
<table class="main-tbl">
  <thead>
    <tr>
      <th>字段路径</th><th>类型</th><th>深度</th><th>状态</th>
      <th>Branch 变化</th><th>关联</th><th>关联 Branch</th>
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
    logger.info(f"explore-all 报告已生成: {output_path}")