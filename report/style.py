

REPORT_CSS_VARS = """\
:root{
  --bg:#f0f4f8;--card:rgba(255,255,255,.78);--card-s:#fff;
  --bdr:rgba(200,210,225,.55);--shadow:0 4px 24px rgba(0,0,0,.06);
  --tx:#1e293b;--tx2:#64748b;--tx3:#94a3b8;
  --acc:#4f46e5;--acc-l:#e0e7ff;
  --ok:#059669;--ok-l:#d1fae5;
  --err:#dc2626;--err-l:#fee2e2;
  --warn:#d97706;--warn-l:#fef3c7;
  --r:14px;--rs:8px;
  --font:-apple-system,'PingFang SC','Microsoft YaHei','Segoe UI',sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--font);background:var(--bg);color:var(--tx);
     min-height:100vh;line-height:1.6;font-size:14px}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:3px}
a{color:var(--acc);text-decoration:none}
"""


REPORT_CSS_HEADER = """\
.rpt-header{background:rgba(255,255,255,.88);backdrop-filter:blur(16px);
  border-bottom:1px solid var(--bdr);padding:18px 28px;display:flex;
  align-items:baseline;gap:12px;position:sticky;top:0;z-index:50}
.rpt-header h1{font-size:18px;font-weight:700;color:var(--tx);letter-spacing:.3px}
.rpt-header .sub{font-size:13px;color:var(--tx2)}
"""


REPORT_CSS_LAYOUT = """\
.rpt-main{max-width:1260px;margin:0 auto;padding:24px 28px}
.card{background:var(--card);backdrop-filter:blur(16px);border:1px solid var(--bdr);
  border-radius:var(--r);box-shadow:var(--shadow);padding:20px;margin-bottom:16px}
h2{font-size:15px;font-weight:600;color:var(--acc);margin:24px 0 10px;
  border-bottom:1px solid var(--bdr);padding-bottom:6px}
h3{font-size:13px;font-weight:600;color:var(--tx2);margin:14px 0 6px}
"""


REPORT_CSS_STATS = """\
.stats-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:16px}
.stat-card{background:var(--card-s);border:1px solid var(--bdr);border-radius:var(--rs);
  padding:14px 16px;text-align:center}
.stat-val{font-size:28px;font-weight:700;color:var(--acc);line-height:1.2}
.stat-label{font-size:11px;color:var(--tx2);margin-top:2px}
.stat-bar{height:4px;border-radius:2px;background:var(--bdr);margin-top:6px}
.stat-bar-fill{height:100%;border-radius:2px;background:var(--acc)}
"""


REPORT_CSS_TABLE = """\
.rpt-tbl{width:100%;border-collapse:collapse;font-size:13px}
.rpt-tbl th{background:rgba(79,70,229,.04);padding:10px 12px;text-align:left;
  color:var(--tx2);font-size:11px;font-weight:600;text-transform:uppercase;
  letter-spacing:.5px;border-bottom:2px solid var(--bdr);white-space:nowrap;
  cursor:pointer;user-select:none}
.rpt-tbl th:hover{color:var(--acc)}
.rpt-tbl td{padding:8px 12px;border-bottom:1px solid rgba(200,210,225,.35);vertical-align:top}
.rpt-tbl tbody tr:hover{background:rgba(79,70,229,.03)}
.mono{font-family:'Cascadia Code','JetBrains Mono',Consolas,monospace;font-size:12px}
.small{font-size:11px}
"""


REPORT_CSS_BADGES = """\
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;margin:0 2px}
.tp-ok{color:var(--ok)}.tp-miss{color:var(--err)}
.tp-ok.badge{background:var(--ok-l)}.tp-miss.badge{background:var(--err-l)}
.badge-new{background:var(--acc-l);color:var(--acc);padding:2px 8px;border-radius:10px;font-size:11px}
.badge-tgt{background:#f1f5f9;color:var(--tx2);padding:2px 8px;border-radius:10px;font-size:11px}
"""


REPORT_CSS_TABS = """\
.tab-nav{display:flex;gap:0;border-bottom:2px solid #e2e8f0;margin-bottom:20px}
.tab-btn{padding:10px 22px;font-size:13px;font-weight:500;color:var(--tx2);cursor:pointer;
  border:none;background:none;border-bottom:2px solid transparent;margin-bottom:-2px;
  transition:all .15s;font-family:var(--font)}
.tab-btn:hover{color:var(--tx)}.tab-btn.active{color:var(--acc);border-bottom-color:var(--acc)}
.tab-panel{display:none}.tab-panel.active{display:block}
"""


REPORT_CSS_CODE = """\
.cr-pre{background:#f8fafc;border:1px solid var(--bdr);border-radius:var(--rs);
  padding:12px;font-size:12px;white-space:pre-wrap;word-break:break-all;
  max-height:400px;overflow-y:auto;font-family:'Cascadia Code',Consolas,monospace;color:var(--tx)}
.diff-pre{background:#f8fafc;border:1px solid var(--bdr);border-radius:var(--rs);
  padding:12px;font-size:12px;font-family:'Cascadia Code',Consolas,monospace;
  max-height:400px;overflow-y:auto}
.diff-pre .dh{color:var(--tx2)}.diff-pre .dc{color:var(--acc)}
.diff-pre .da{color:var(--ok);background:var(--ok-l)}
.diff-pre .dr{color:var(--err);background:var(--err-l)}
.diff-pre .dx{color:var(--tx)}
details summary{cursor:pointer;color:var(--acc);font-size:12px;font-weight:500}
"""


REPORT_CSS_CHART = """\
.chart-wrap{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.chart-card{background:var(--card-s);border:1px solid var(--bdr);border-radius:var(--rs);padding:16px}
.chart-card h3{font-size:13px;color:var(--tx2);margin-bottom:10px;border:none;padding:0}
canvas{max-height:220px}
@media(max-width:700px){.chart-wrap{grid-template-columns:1fr}}
"""


REPORT_CSS_COLLAPSE = """\
.fc-card{border:1px solid var(--bdr);border-radius:var(--rs);margin-bottom:8px;overflow:hidden}
.fc-hdr{background:var(--card-s);padding:8px 14px;display:flex;align-items:center;gap:10px;
  cursor:pointer;user-select:none;flex-wrap:wrap;transition:background .15s}
.fc-hdr:hover{background:rgba(79,70,229,.04)}
.fc-toggle{color:var(--tx3);font-size:12px;transition:transform .2s;margin-left:auto}
.fc-hdr.open .fc-toggle{transform:rotate(180deg)}
.fc-body{display:none;overflow-x:auto;background:#f8fafc;padding:10px 14px;border-top:1px solid var(--bdr)}
.fc-body.open{display:block}
"""


REPORT_CSS_P1 = """\
.p1-row{display:flex;gap:10px;align-items:center;padding:6px 8px;border-bottom:1px solid var(--bdr)}
.p1-field{font-family:'Cascadia Code',Consolas,monospace;color:var(--acc);min-width:220px;font-size:12px}
.p1-status{font-weight:600;min-width:90px;font-size:12px}
.p1-ok .p1-status{color:var(--ok)}.p1-fail .p1-status{color:var(--err)}
.p1-detail{color:var(--tx2);font-size:11px}
"""


REPORT_CSS_EXPLORE = """\
.cm-grid{display:flex;flex-wrap:wrap;gap:2px;margin-bottom:16px}
.cm-cell{width:8px;height:8px;border-radius:2px}
.cm-both{background:var(--ok)}.cm-true{background:var(--warn)}
.cm-false{background:var(--acc)}.cm-none{background:#e2e8f0}
.log-combo{font-family:'Cascadia Code',Consolas,monospace;color:var(--acc);font-size:12px}
.log-targets{color:var(--tx2);font-size:11px}
.log-status{margin-left:auto;font-weight:600;font-size:12px;border-radius:4px;padding:0 6px}
.log-success{background:var(--ok-l);color:var(--ok)}.log-fail{background:var(--err-l);color:var(--err)}
.att-block{border-bottom:1px solid var(--bdr);padding:4px 0}
.att-block:last-child{border-bottom:none}
.att-row{display:flex;gap:6px;align-items:center;font-size:12px;padding:2px 0}
.att-n{color:var(--tx3);font-family:monospace;min-width:24px}
.att-lbl{font-weight:600;border-radius:4px;padding:1px 6px;font-size:11px}
.att-ok .att-lbl{background:var(--ok-l);color:var(--ok)}
.att-err .att-lbl{background:var(--err-l);color:var(--err)}
.att-miss .att-lbl{background:var(--warn-l);color:var(--warn)}
.bv-chip{border-radius:4px;padding:1px 5px;font-family:monospace;font-size:11px}
.bv-hit{background:var(--ok-l);color:var(--ok)}.bv-miss{background:var(--err-l);color:var(--err)}
.att-err-msg{color:var(--err);font-style:italic;font-size:11px}
.section-label{font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:var(--tx3);margin:8px 0 3px}
.att-prompt-toggle{font-size:11px;color:var(--acc);cursor:pointer;user-select:none;margin:6px 0 2px;display:inline-block}
.att-prompt-toggle:hover{text-decoration:underline}
"""


REPORT_CSS_LLM = """\
.llm-card{background:var(--card-s);border:1px solid var(--bdr);border-radius:var(--rs);padding:16px;margin-bottom:16px}
.llm-row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--bdr)}
.llm-row:last-child{border-bottom:none}
.llm-lbl{color:var(--tx2);font-size:13px}.llm-val{font-size:13px;font-weight:600}
"""


REPORT_CSS_COVTEST = """\
.ct-card{border:1px solid var(--bdr);border-radius:var(--rs);margin-bottom:12px;overflow:hidden}
.ct-hdr{padding:10px 16px;display:flex;align-items:center;gap:10px;cursor:pointer;user-select:none}
.ct-ok .ct-hdr{background:var(--ok-l)}.ct-fail .ct-hdr{background:var(--err-l)}
.ct-bi{font-family:monospace;font-weight:700;color:var(--acc)}
.ct-lbl{font-weight:600;font-size:12px}
.ct-ok .ct-lbl{color:var(--ok)}.ct-fail .ct-lbl{color:var(--err)}
.ct-cond{color:var(--tx2);font-size:12px;flex:1}
.ct-body{display:none;padding:12px 16px;background:#f8fafc;border-top:1px solid var(--bdr)}
.ct-body.open{display:block}
"""


REPORT_CSS_MISC = """\
.muted{color:var(--tx3);font-size:12px;margin-top:6px}
.empty-msg{color:var(--tx3);font-style:italic;padding:16px}
footer{margin-top:28px;color:var(--tx3);font-size:11px;text-align:center;padding:16px 0;
  border-top:1px solid var(--bdr)}
"""


REPORT_CHART_JS_CONFIG = """\
const chartDefaults = {
  type: 'line',
  options: {
    animation: false,
    responsive: true,
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: '#64748b', maxTicksLimit: 12 }, grid: { color: 'rgba(200,210,225,.4)' } },
      y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(200,210,225,.4)' }, beginAtZero: true }
    }
  }
};
"""


REPORT_SORTABLE_JS = """\
document.querySelectorAll('table.sortable thead th').forEach((th, idx) => {
  th.addEventListener('click', () => {
    const tbody = th.closest('table').querySelector('tbody');
    const rows = [...tbody.rows];
    const asc = th.dataset.asc !== 'true';
    th.dataset.asc = asc;
    rows.sort((a, b) => {
      const av = a.cells[idx]?.textContent.trim() ?? '';
      const bv = b.cells[idx]?.textContent.trim() ?? '';
      const an = parseFloat(av), bn = parseFloat(bv);
      if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
      return asc ? av.localeCompare(bv) : bv.localeCompare(av);
    });
    rows.forEach(r => tbody.appendChild(r));
  });
});
"""


REPORT_TOGGLE_JS = """\
function toggleCard(hdr) {
  hdr.classList.toggle('open');
  hdr.nextElementSibling.classList.toggle('open');
}
function toggleNext(el) {
  const pre = el.nextElementSibling;
  if (!pre) return;
  const hidden = pre.style.display === 'none';
  pre.style.display = hidden ? 'block' : 'none';
  el.textContent = el.textContent.replace(/^[▶▼]/, hidden ? '▼' : '▶');
}
"""


def full_report_css() -> str:
    """Return a complete CSS string for a GSOD report page."""
    return (
        REPORT_CSS_VARS
        + REPORT_CSS_HEADER
        + REPORT_CSS_LAYOUT
        + REPORT_CSS_STATS
        + REPORT_CSS_TABLE
        + REPORT_CSS_BADGES
        + REPORT_CSS_TABS
        + REPORT_CSS_CODE
        + REPORT_CSS_CHART
        + REPORT_CSS_COLLAPSE
        + REPORT_CSS_P1
        + REPORT_CSS_EXPLORE
        + REPORT_CSS_LLM
        + REPORT_CSS_COVTEST
        + REPORT_CSS_MISC
    )