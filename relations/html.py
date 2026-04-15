import json
import logging
import os
from datetime import datetime

from instrumentation.diff import _build_branch_index

logger = logging.getLogger(__name__)


def generate_relations_html(
    relations: dict,
    instrument_info_path: str,
    output_path: str,
    page_size: int = 20,
    context_path: str = "",
):
    """生成 field_relations 对应的 HTML 可视化报告（适配新数据结构）。

    所有字段数据以 JSON 嵌入页面，由 JS 按需渲染当前分页，
    避免一次性创建大量 DOM 节点导致页面卡死。
    默认折叠所有卡片，支持分页、搜索、筛选。
    """
    branch_meta = _build_branch_index(instrument_info_path)
    fields = sorted(relations.keys())

    total_fields = len(fields)

    unique_branches = len(
        {bi for f in fields for bi in relations[f].get("branch_indices", [])}
    )

    fields_with_vars = sum(
        1
        for f in fields
        if any(
            relations[f].get("variable_mappings", {}).get(str(bi))
            for bi in relations[f].get("branch_indices", [])
        )
    )
    total_instr = len(branch_meta)

    total_crd_fields = 0
    if context_path and os.path.exists(context_path):
        try:
            import sys as _sys

            _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if _root not in _sys.path:
                _sys.path.insert(0, _root)
            from crd.schema import extract_crd_spec_fields

            total_crd_fields = len(extract_crd_spec_fields(context_path))
        except Exception as _e:
            logger.warning(f"无法读取 context.json 中的 CRD 字段: {_e}")


    fields_data = []
    for fp in fields:
        fdata = relations[fp]
        branch_indices = fdata.get("branch_indices", [])
        evidence = fdata.get("evidence", {})
        var_mappings = fdata.get("variable_mappings", {})
        branches = []
        for bi in sorted(branch_indices):
            bp = branch_meta.get(bi, {})
            bi_vm = var_mappings.get(str(bi), {})
            var_list = []
            for vkey, vinfo in sorted(bi_vm.items()):
                var_list.append(
                    {
                        "fmt": vinfo.get("variable_fmt", vkey),
                        "kind": vinfo.get("variable_kind", ""),
                        "evidence": [
                            {
                                "bv": str(e.get("before_value", "")),
                                "av": str(e.get("after_value", "")),
                                "mut": str(e.get("mutation", "")),
                            }
                            for e in vinfo.get("evidence", [])[:3]
                        ],
                    }
                )
            evid_list = evidence.get(str(bi), evidence.get(bi, []))
            br_evid = [
                {
                    "mut": str(e.get("mutation", "?")),
                    "change": e.get("change", ""),
                    "bv": str(e.get("before_value", "")) if "before_value" in e else "",
                    "av": str(e.get("after_value", "")) if "after_value" in e else "",
                }
                for e in evid_list[:5]
            ]
            branches.append(
                {
                    "bi": bi,
                    "file": bp.get("File", ""),
                    "line": bp.get("Line", ""),
                    "func": bp.get("Func", ""),
                    "kind": bp.get("Kind", ""),
                    "cond": bp.get("Fmt") or bp.get("Raw", ""),
                    "vars": var_list,
                    "evid": br_evid,
                }
            )
        fields_data.append(
            {
                "fp": fp,
                "ft": fdata.get("field_type", ""),
                "total": fdata.get("total_branches", len(branch_indices)),
                "ts": fdata.get("last_updated", "")[:19],
                "branches": branches,
            }
        )

    fields_json = json.dumps(fields_data, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8"/>
  <title>GSOD — Field Relations</title>
  <style>
    :root {{
      --bg:#0f1117;--card:#1a1d27;--border:#2d3146;
      --accent:#6c8cff;--green:#4ade80;--red:#f87171;
      --yellow:#fbbf24;--purple:#a78bfa;
      --text:#e2e8f0;--muted:#94a3b8;
    }}
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;
         font-size:14px;line-height:1.6;padding:20px}}
    h1{{color:var(--accent);font-size:1.5rem;margin-bottom:4px}}
    .subtitle{{color:var(--muted);font-size:.9rem;font-weight:400;margin-bottom:20px}}
    .stats{{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}}
    .stat{{background:var(--card);border:1px solid var(--border);border-radius:8px;
           padding:10px 18px;text-align:center;min-width:100px}}
    .stat .v{{font-size:1.8rem;font-weight:700;color:var(--accent)}}
    .stat .l{{font-size:.72rem;color:var(--muted);margin-top:2px}}
    .controls{{display:flex;gap:8px;margin-bottom:16px;align-items:center;flex-wrap:wrap}}
    .controls button{{background:var(--card);color:var(--text);border:1px solid var(--border);
                      border-radius:6px;padding:5px 12px;cursor:pointer;font-size:12px}}
    .controls button:hover{{background:var(--border)}}
    #search{{background:var(--card);color:var(--text);border:1px solid var(--border);
             border-radius:6px;padding:5px 10px;font-size:12px;width:240px}}
    select#pgsize{{background:var(--card);color:var(--text);border:1px solid var(--border);
                   border-radius:6px;padding:5px 8px;font-size:12px}}
    .pg-info{{color:var(--muted);font-size:12px;margin-left:auto}}
    .pagination{{display:flex;gap:6px;align-items:center;margin-top:16px;flex-wrap:wrap}}
    .pagination button{{background:var(--card);color:var(--text);border:1px solid var(--border);
                        border-radius:6px;padding:4px 10px;cursor:pointer;font-size:12px;min-width:32px}}
    .pagination button:hover{{background:var(--border)}}
    .pagination button.active{{background:var(--accent);color:#fff;border-color:var(--accent)}}
    .pagination button:disabled{{opacity:.35;cursor:default}}
    .field-card{{border:1px solid var(--border);border-radius:10px;margin-bottom:12px;overflow:hidden}}
    .fc-header{{background:var(--card);padding:10px 16px;display:flex;align-items:center;
                gap:12px;flex-wrap:wrap;cursor:pointer;user-select:none;transition:background .15s}}
    .fc-header:hover{{background:#1f2336}}
    .fc-path{{font-family:monospace;font-weight:600;color:var(--accent);font-size:13px}}
    .fc-type{{background:#1a1a30;color:var(--purple);border-radius:4px;padding:0 6px;
              font-size:11px;font-family:monospace}}
    .fc-count{{margin-left:auto;background:#14291a;color:var(--green);
               border-radius:4px;padding:1px 8px;font-size:12px;font-weight:600}}
    .fc-ts{{color:var(--muted);font-size:11px}}
    .fc-toggle{{color:var(--muted);font-size:12px;transition:transform .2s}}
    .fc-header.open .fc-toggle{{transform:rotate(180deg)}}
    .fc-body{{display:none;overflow-x:auto;background:var(--bg)}}
    .fc-body.open{{display:block}}
    table.br-table{{width:100%;border-collapse:collapse;font-size:12px}}
    .br-table th{{background:#12141e;color:var(--muted);padding:5px 10px;text-align:left;
                  font-size:10px;text-transform:uppercase;letter-spacing:.04em;
                  border-bottom:1px solid var(--border)}}
    .br-table td{{padding:6px 10px;border-bottom:1px solid #1e2235;vertical-align:top}}
    .br-table tr:last-child td{{border-bottom:none}}
    .br-table tr:hover td{{background:#141726}}
    .br-idx{{font-family:monospace;font-weight:700;color:var(--yellow);text-align:right;width:52px}}
    .br-th-idx{{width:52px}} .br-th-kind{{width:50px}}
    .br-th-loc{{width:28%}} .br-th-cond{{width:28%}}
    .br-file{{color:var(--muted);font-family:monospace;font-size:11px;display:block}}
    .br-func{{color:var(--accent);font-family:monospace;font-size:11px;display:block}}
    .br-kind{{font-size:10px;background:var(--card);color:var(--purple);
              border-radius:3px;padding:1px 4px;font-family:monospace}}
    .br-cond code{{font-family:'Consolas',monospace;font-size:11px;color:var(--text);
                   white-space:pre-wrap;word-break:break-all}}
    .no-meta{{color:var(--muted);font-style:italic;font-size:11px}}
    .br-evid{{min-width:220px}}
    .evrow{{margin-bottom:4px;display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;font-size:11px}}
    .evmut{{background:var(--card);color:var(--muted);border-radius:3px;padding:0 4px;
            font-family:monospace;flex-shrink:0}}
    .evchg{{font-weight:600;border-radius:3px;padding:0 5px;flex-shrink:0}}
    .ev-added{{background:#14291a;color:var(--green)}}
    .ev-removed{{background:#2a1414;color:var(--red)}}
    .ev-changed{{background:#1e1e0a;color:var(--yellow)}}
    .ev-bv{{color:var(--red);font-family:monospace}}
    .ev-av{{color:var(--green);font-family:monospace}}
    .evpred{{display:inline-flex;align-items:baseline;gap:3px;background:#12141e;
             border-radius:3px;padding:1px 5px;margin:1px;font-family:monospace}}
    .ev-type{{color:var(--muted);font-size:10px;background:var(--card);
              border-radius:2px;padding:0 3px;margin-left:4px}}
    .no-evid{{color:var(--muted);font-style:italic}}
    .filter-bar{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}}
    .filter-bar label{{color:var(--muted);font-size:12px}}
    .filter-bar input[type=range]{{accent-color:var(--accent);width:100px}}
    .filter-bar span{{color:var(--accent);font-size:12px;min-width:24px}}
    footer{{margin-top:28px;color:var(--muted);font-size:11px;text-align:center}}
  </style>
</head>
<body>
  <h1>GSOD — Field Relations (v5)</h1>
  <p class="subtitle">字段 ↔ Branch 关联映射</p>
  <div class="stats">
    <div class="stat"><div class="v">{total_fields}</div><div class="l">Fields with Associations</div></div>
    {(f'<div class="stat"><div class="v">{total_crd_fields}</div><div class="l">Total CRD Fields</div></div><div class="stat"><div class="v">{round(total_fields / total_crd_fields * 100)}%</div><div class="l">Coverage Ratio</div></div>') if total_crd_fields else ""}
    <div class="stat"><div class="v">{fields_with_vars}</div><div class="l">Fields with Var Mappings</div></div>
    <div class="stat"><div class="v">{unique_branches}</div><div class="l">Unique Branches Linked</div></div>
    <div class="stat"><div class="v">{total_instr}</div><div class="l">Total Instrumented Branches</div></div>
  </div>
  <div class="filter-bar">
    <label>最少分支数：</label>
    <input type="range" id="minbr" min="0" max="50" value="0" oninput="onMinBrChange(this.value)"/>
    <span id="minbr-val">0</span>
    <label style="margin-left:12px">最多分支数（0=不限）：</label>
    <input type="range" id="maxbr" min="0" max="200" value="0" oninput="onMaxBrChange(this.value)"/>
    <span id="maxbr-val">不限</span>
  </div>
  <div class="controls">
    <button onclick="expandPage()">展开当前页</button>
    <button onclick="collapsePage()">收起当前页</button>
    <input id="search" placeholder="搜索字段名..." oninput="onSearch(this.value)"/>
    <label style="color:var(--muted);font-size:12px;margin-left:8px">每页</label>
    <select id="pgsize" onchange="onPgSizeChange(this.value)">
      <option value="10">10</option>
      <option value="20" selected>20</option>
      <option value="50">50</option>
      <option value="100">100</option>
    </select>
    <span class="pg-info" id="pg-info"></span>
  </div>
  <div id="cards"></div>
  <div class="pagination" id="pagination"></div>
  <footer>生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} · instrument_info: {instrument_info_path}</footer>

<script>
const ALL_FIELDS = {fields_json};
const PAGE_SIZE_DEFAULT = {page_size};

let filtered = ALL_FIELDS.slice();
let currentPage = 1;
let pageSize = PAGE_SIZE_DEFAULT;
let searchQ = '';
let minBr = 0;
let maxBr = 0;

function esc(s) {{
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}}

function renderBranchRows(branches) {{
  if (!branches.length) return '<tr><td colspan="5" style="color:var(--muted);font-style:italic;padding:8px 10px">无分支数据</td></tr>';
  return branches.map(b => {{
    const loc = b.file ? esc(b.file + ':' + b.line) : 'branch[' + b.bi + ']';
    const noMeta = !b.file ? '<span class="no-meta">（无源码信息）</span>' : '';
    const funcSpan = b.func ? '<span class="br-func">' + esc(b.func) + '</span>' : '';
    // variable mappings
    let varRows = b.vars.map(v => {{
      const evCells = v.evidence.map(e =>
        '<span class="evpred">' +
        '<span class="ev-bv">' + esc(e.bv||'—') + '</span> → ' +
        '<span class="ev-av">' + esc(e.av||'—') + '</span>' +
        '<span class="ev-type">' + esc(e.mut) + '</span>' +
        '</span>'
      ).join('');
      return '<div class="evrow"><span class="evmut">`' + esc(v.fmt) + '`</span>' +
             '<span class="br-kind">' + esc(v.kind) + '</span>' + evCells + '</div>';
    }}).join('');
    // branch-level evidence
    const chgCls = {{added:'ev-added',removed:'ev-removed',changed:'ev-changed'}};
    let evidRows = b.evid.map(e => {{
      const valDiff = (e.bv||e.av) ?
        '<span class="ev-bv">' + esc(e.bv||'—') + '</span> → <span class="ev-av">' + esc(e.av||'—') + '</span>' : '';
      return '<div class="evrow"><span class="evmut">mut#' + esc(e.mut) + '</span>' +
             '<span class="evchg ' + (chgCls[e.change]||'') + '">' + esc(e.change) + '</span>' +
             valDiff + '</div>';
    }}).join('');
    const evidContent = (varRows || evidRows) || '<span class="no-evid">—</span>';
    return '<tr class="br-row">' +
      '<td class="br-idx">' + b.bi + '</td>' +
      '<td class="br-loc"><span class="br-file" title="' + esc(b.file) + '">' + loc + '</span>' + funcSpan + noMeta + '</td>' +
      '<td><span class="br-kind">' + esc(b.kind) + '</span></td>' +
      '<td class="br-cond"><code>' + esc(b.cond) + '</code></td>' +
      '<td class="br-evid">' + evidContent + '</td>' +
      '</tr>';
  }}).join('');
}}

function renderCard(fd) {{
  const id = 'fc-' + fd.fp.replace(/[^a-zA-Z0-9]/g,'_');
  const rows = renderBranchRows(fd.branches);
  return '<div class="field-card" id="' + id + '">' +
    '<div class="fc-header" onclick="toggleCard(this)">' +
    '<span class="fc-path">' + esc(fd.fp) + '</span>' +
    (fd.ft ? '<span class="fc-type">' + esc(fd.ft) + '</span>' : '') +
    '<span class="fc-count">' + fd.total + ' branches</span>' +
    '<span class="fc-ts">' + esc(fd.ts) + '</span>' +
    '<span class="fc-toggle">▼</span>' +
    '</div>' +
    '<div class="fc-body">' +
    '<table class="br-table"><thead><tr>' +
    '<th class="br-th-idx">Index</th>' +
    '<th class="br-th-loc">Source Location</th>' +
    '<th class="br-th-kind">Kind</th>' +
    '<th class="br-th-cond">Condition</th>' +
    '<th class="br-th-evid">Variable Mappings &amp; Evidence</th>' +
    '</tr></thead><tbody>' + rows + '</tbody></table>' +
    '</div></div>';
}}

function applyFilters() {{
  filtered = ALL_FIELDS.filter(fd => {{
    if (searchQ && !fd.fp.toLowerCase().includes(searchQ)) return false;
    if (minBr > 0 && fd.total < minBr) return false;
    if (maxBr > 0 && fd.total > maxBr) return false;
    return true;
  }});
  currentPage = 1;
  renderPage();
}}

function renderPage() {{
  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  if (currentPage > totalPages) currentPage = totalPages;
  const start = (currentPage - 1) * pageSize;
  const slice = filtered.slice(start, start + pageSize);

  document.getElementById('cards').innerHTML = slice.map(renderCard).join('');
  document.getElementById('pg-info').textContent =
    '第 ' + currentPage + '/' + totalPages + ' 页，共 ' + total + ' 个字段';

  // pagination buttons
  const pg = document.getElementById('pagination');
  let btns = '';
  btns += '<button onclick="goPage(' + (currentPage-1) + ')"' + (currentPage<=1?' disabled':'') + '>‹ 上一页</button>';
  // page number buttons (window of ±3)
  const lo = Math.max(1, currentPage-3), hi = Math.min(totalPages, currentPage+3);
  if (lo > 1) btns += '<button onclick="goPage(1)">1</button>' + (lo>2?'<span style="color:var(--muted);padding:0 4px">…</span>':'');
  for (let p = lo; p <= hi; p++) {{
    btns += '<button onclick="goPage(' + p + ')"' + (p===currentPage?' class="active"':'') + '>' + p + '</button>';
  }}
  if (hi < totalPages) btns += (hi<totalPages-1?'<span style="color:var(--muted);padding:0 4px">…</span>':'') + '<button onclick="goPage(' + totalPages + ')">' + totalPages + '</button>';
  btns += '<button onclick="goPage(' + (currentPage+1) + ')"' + (currentPage>=totalPages?' disabled':'') + '>下一页 ›</button>';
  pg.innerHTML = btns;
}}

function goPage(p) {{
  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  currentPage = Math.max(1, Math.min(totalPages, p));
  renderPage();
  window.scrollTo(0,0);
}}

function toggleCard(hdr) {{
  hdr.classList.toggle('open');
  hdr.nextElementSibling.classList.toggle('open');
}}
function expandPage() {{
  document.querySelectorAll('.fc-header').forEach(h => {{
    h.classList.add('open'); h.nextElementSibling.classList.add('open');
  }});
}}
function collapsePage() {{
  document.querySelectorAll('.fc-header').forEach(h => {{
    h.classList.remove('open'); h.nextElementSibling.classList.remove('open');
  }});
}}

function onSearch(q) {{ searchQ = q.trim().toLowerCase(); applyFilters(); }}
function onPgSizeChange(v) {{ pageSize = parseInt(v); applyFilters(); }}
function onMinBrChange(v) {{
  minBr = parseInt(v);
  document.getElementById('minbr-val').textContent = v;
  applyFilters();
}}
function onMaxBrChange(v) {{
  maxBr = parseInt(v);
  document.getElementById('maxbr-val').textContent = maxBr > 0 ? v : '不限';
  applyFilters();
}}

// Initial render
renderPage();
</script>
</body></html>"""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"field_relations HTML 已生成: {output_path}")