"""
Spectacle report generator.

Produces a self-contained HTML report from all sprint findings:
  - Anomaly summary
  - CapEx spike evidence
  - QFG gap analysis
  - ClinicalTrials enrollment data
  - Earnings call evasion evidence
  - Verdict
"""

from datetime import datetime
from pathlib import Path

from logger import get_logger

log = get_logger(__name__)


def generate_report(
    ticker: str,
    company_name: str,
    anomalies: list[dict],
    capex_df,
    qfg_results: dict,
    trials: list[dict],
    qa_findings: list[dict],
    output_path: str | Path,
    filing_analysis: list[dict] | None = None,
) -> Path:
    out = Path(output_path)
    out.parent.mkdir(exist_ok=True)

    flat_trials    = [t for t in trials if t["flat_signal"]]
    evasive_qa     = [f for f in qa_findings if f["evasion_score"] > 0]
    all_qa_capex   = [f for f in qa_findings]
    filing_analysis = filing_analysis or []
    generated_at   = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Sprint Report — {ticker}</title>
<style>
  :root {{
    --bg: #ffffff; --surface: #f6f8fa; --border: #d0d7de;
    --text: #1f2328; --muted: #656d76; --red: #cf222e;
    --green: #1a7f37; --blue: #0969da; --yellow: #9a6700;
    --mono: 'JetBrains Mono','Fira Code',monospace;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         font-size: 14px; color: var(--text); background: var(--bg); line-height: 1.6; }}
  .page {{ max-width: 960px; margin: 0 auto; padding: 40px 32px; }}

  /* cover */
  .cover {{ border-bottom: 3px solid var(--red); padding-bottom: 32px; margin-bottom: 40px; }}
  .cover-label {{ font-size: 11px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase;
                  color: var(--red); margin-bottom: 8px; }}
  .cover h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 6px; }}
  .cover .subtitle {{ font-size: 16px; color: var(--muted); margin-bottom: 20px; }}
  .cover-meta {{ display: flex; gap: 32px; font-size: 12px; color: var(--muted); }}
  .cover-meta strong {{ color: var(--text); }}

  /* sections */
  h2 {{ font-size: 18px; font-weight: 700; margin: 40px 0 16px; padding-bottom: 8px;
        border-bottom: 1px solid var(--border); }}
  h3 {{ font-size: 14px; font-weight: 700; margin: 20px 0 8px; color: var(--muted);
        text-transform: uppercase; letter-spacing: .06em; font-size: 11px; }}

  /* verdict banner */
  .verdict {{ background: #fff8c5; border: 1px solid #d4a72c; border-radius: 6px;
              padding: 16px 20px; margin: 24px 0; }}
  .verdict.anomaly {{ background: #ffebe9; border-color: #ff8182; }}
  .verdict strong {{ font-size: 15px; }}

  /* tables */
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin: 12px 0; }}
  th {{ background: var(--surface); text-align: left; padding: 8px 12px;
        font-size: 11px; font-weight: 700; letter-spacing: .05em; text-transform: uppercase;
        border-bottom: 2px solid var(--border); color: var(--muted); }}
  td {{ padding: 8px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  .anomaly-row {{ background: #ffebe9; font-weight: 700; }}
  .flat-row {{ background: #fff8c5; }}

  /* evidence blocks */
  .evidence {{ background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
               padding: 16px; margin: 12px 0; font-size: 13px; }}
  .evidence .q {{ color: var(--blue); font-weight: 600; margin-bottom: 6px; }}
  .evidence .a {{ color: var(--text); }}
  .evidence .a .hit {{ background: #fff8c5; padding: 1px 3px; border-radius: 3px; font-weight: 700; color: var(--yellow); }}
  .evidence .meta {{ font-size: 11px; color: var(--muted); margin-top: 8px; }}

  /* badges */
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 700; }}
  .badge-red    {{ background: #ffebe9; color: var(--red); }}
  .badge-green  {{ background: #dafbe1; color: var(--green); }}
  .badge-yellow {{ background: #fff8c5; color: var(--yellow); }}
  .badge-blue   {{ background: #ddf4ff; color: var(--blue); }}

  .mono {{ font-family: var(--mono); font-size: 12px; }}
  a {{ color: var(--blue); }}
  .empty {{ color: var(--muted); font-style: italic; padding: 12px 0; }}
  footer {{ margin-top: 60px; padding-top: 20px; border-top: 1px solid var(--border);
            font-size: 11px; color: var(--muted); }}
</style>
</head>
<body>
<div class="page">

<!-- ── Cover ── -->
<div class="cover">
  <div class="cover-label">Sprint Research Report — Confidential</div>
  <h1>{company_name} ({ticker})</h1>
  <div class="subtitle">CapEx Spike / Forward Guidance Anomaly Analysis</div>
  <div class="cover-meta">
    <div><strong>Generated:</strong> {generated_at}</div>
    <div><strong>Anomaly quarters:</strong> {", ".join(a["quarter"] for a in anomalies) or "None"}</div>
    <div><strong>Trials flagged:</strong> {len(flat_trials)} of {len(trials)}</div>
    <div><strong>Evasive Q&amp;A:</strong> {len(evasive_qa)} instance(s)</div>
    <div><strong>Filing context:</strong> {sum(1 for fa in filing_analysis if fa["explanation_score"] > 0)} quarter(s) with context</div>
  </div>
</div>

<!-- ── Verdict ── -->
{_verdict_html(anomalies, flat_trials, evasive_qa)}

<!-- ── Section 1: Signal ── -->
<h2>1. Anomaly Signal</h2>
<p style="color:var(--muted);margin-bottom:12px">
  A quarter satisfies the signal when <strong>Condition A</strong> (CapEx QoQ spike ≥40%)
  <em>and</em> <strong>Condition B</strong> (no quantified forward guidance in 8-K filings) are both true.
</p>
{_capex_table(capex_df, qfg_results)}

<!-- ── Section 2: QFG Gap ── -->
<h2>2. Forward Guidance Analysis</h2>
{_qfg_section(anomalies, qfg_results)}

<!-- ── Section 3: Clinical Trials ── -->
<h2>3. Clinical Trial Enrollment</h2>
{_trials_section(trials, flat_trials)}

<!-- ── Section 4: Earnings Call Q&A ── -->
<h2>4. Earnings Call Capital Allocation Q&amp;A</h2>
{_qa_section(all_qa_capex, evasive_qa, ticker)}

<!-- ── Section 5: Press Release Analysis ── -->
<h2>5. Press Release Analysis (EX-99.1)</h2>
{_filing_analysis_section(filing_analysis)}

<!-- ── Section 6: Conclusion ── -->
<h2>6. Conclusion</h2>
{_conclusion(ticker, company_name, anomalies, flat_trials, evasive_qa, filing_analysis)}

{_continue_research_button(ticker, company_name, anomalies)}

<footer>
  Generated by exogesisdoer sprint tool &nbsp;·&nbsp; {generated_at} &nbsp;·&nbsp;
  Source data: SEC EDGAR, ClinicalTrials.gov (public) &nbsp;·&nbsp;
  This report is for research purposes only and does not constitute investment advice.
</footer>

</div>
</body>
</html>"""

    out.write_text(html, encoding="utf-8")
    log.info("Report written → %s (%d bytes)", out, len(html))
    return out


# ── section renderers ─────────────────────────────────────────────────────────

def _verdict_html(anomalies, flat_trials, evasive_qa) -> str:
    if not anomalies:
        return '<div class="verdict"><strong>No anomaly quarters found.</strong> Both conditions were not simultaneously met in any quarter.</div>'
    parts = [f"<strong>★ ANOMALY DETECTED</strong> — {len(anomalies)} quarter(s) satisfy both conditions."]
    if flat_trials:
        parts.append(f"<br>{len(flat_trials)} clinical trial(s) show flat/concerning enrollment — consistent with capital being misallocated.")
    if evasive_qa:
        parts.append(f"<br>{len(evasive_qa)} evasive management response(s) to capital allocation questions identified.")
    return f'<div class="verdict anomaly">{" ".join(parts)}</div>'


def _capex_table(df, qfg_results) -> str:
    if df is None or df.empty:
        return '<p class="empty">No CapEx data available.</p>'
    rows = ""
    for _, row in df.iterrows():
        pe    = row["period_end"]
        q     = _ql(pe)
        val   = f"${row['value_usd']/1e6:.1f}M"
        qoq   = f"{row['qoq_pct']:+.1f}%" if row["qoq_pct"] == row["qoq_pct"] else "N/A"
        spike = '<span class="badge badge-red">YES</span>' if row["spike"] else '<span class="badge badge-green">no</span>'
        qfg   = qfg_results.get(pe, {})
        flag  = qfg.get("flag", "?")
        flag_html = '<span class="badge badge-red">0 — missing</span>' if flag == 0 else f'<span class="badge badge-green">1 — present</span>'
        anomaly = row["spike"] and flag == 0
        css   = 'class="anomaly-row"' if anomaly else ""
        star  = " ★" if anomaly else ""
        rows += f"<tr {css}><td class='mono'>{q}{star}</td><td>{val}</td><td>{qoq}</td><td>{spike}</td><td>{flag_html}</td></tr>"
    return f"""<table>
<thead><tr><th>Quarter</th><th>CapEx</th><th>QoQ Change</th><th>Spike ≥40%</th><th>QFG Flag</th></tr></thead>
<tbody>{rows}</tbody>
</table>"""


def _qfg_section(anomalies, qfg_results) -> str:
    if not anomalies:
        return '<p class="empty">No anomaly quarters — QFG gap not applicable.</p>'
    parts = []
    for a in anomalies:
        qfg   = a["qfg"]
        dates = ", ".join(qfg.get("filing_dates", [])) or "none found"
        n     = qfg.get("filings_checked", 0)
        parts.append(f"""
<h3>{a["quarter"]} — QFG = 0</h3>
<p>Searched <strong>{n}</strong> 8-K filing(s) (Items 2.02/7.01) filed after quarter end.<br>
Filing dates checked: <span class="mono">{dates}</span><br>
<strong>No sentence found</strong> containing a currency figure, expansion noun, and future-tense language together.
This means management provided no quantified forward-looking justification for the {a["qoq_pct"]:+.1f}% CapEx increase.</p>
""")
    return "".join(parts)


def _trials_section(trials, flat_trials) -> str:
    if not trials:
        return '<p class="empty">No clinical trials found for this sponsor on ClinicalTrials.gov.</p>'
    rows = ""
    for t in trials:
        css   = 'class="flat-row"' if t["flat_signal"] else ""
        flag  = '<span class="badge badge-red">⚠ flat/concerning</span>' if t["flat_signal"] else '<span class="badge badge-green">active</span>'
        enr   = f"{t['enrollment_count']} ({t['enrollment_type'].lower()})" if t["enrollment_count"] else "—"
        rows += f"""<tr {css}>
<td><a href="{t['url']}" target="_blank" class="mono">{t['nct_id']}</a></td>
<td>{_trunc(t['title'], 60)}</td>
<td>{t['status'].replace('_',' ').title()}</td>
<td>{enr}</td>
<td>{t.get('phase','')}</td>
<td>{flag}</td>
</tr>"""
    return f"""<table>
<thead><tr><th>NCT ID</th><th>Title</th><th>Status</th><th>Enrollment</th><th>Phase</th><th>Signal</th></tr></thead>
<tbody>{rows}</tbody>
</table>"""


def _qa_section(all_qa, evasive_qa, ticker) -> str:
    if not all_qa:
        return f'<p class="empty">No capital-allocation Q&amp;A blocks found in 8-K filings for {ticker}.</p>'

    parts = [f"<p style='color:var(--muted);margin-bottom:16px'>{len(all_qa)} capital-allocation Q&amp;A block(s) found across all scanned filings. {len(evasive_qa)} flagged as evasive.</p>"]
    for f in all_qa:
        score   = f["evasion_score"]
        badge   = f'<span class="badge badge-red">evasion score: {score}</span>' if score > 0 else f'<span class="badge badge-green">no evasion detected</span>'
        a_html  = _highlight_evasion(f["answer"])
        parts.append(f"""<div class="evidence">
<div class="q">Q ({f['quarter']} · {f['filing_date']}) &nbsp; {badge}</div>
<div style="color:var(--muted);margin-bottom:8px;font-size:13px">{_trunc(f['question'], 300)}</div>
<div class="a">{a_html}</div>
<div class="meta"><a href="{f['url']}" target="_blank">SEC filing {f['accession']}</a></div>
</div>""")
    return "".join(parts)


def _filing_analysis_section(filing_analysis: list[dict]) -> str:
    if not filing_analysis:
        return '<p class="empty">No press release analysis available.</p>'

    score_labels = ["No context found", "Partial context — facility mentioned", "Explicit disclosure — dollar amount + facility"]
    score_badges = ["badge-red", "badge-yellow", "badge-green"]
    parts = []
    for fa in filing_analysis:
        s = fa["explanation_score"]
        badge = f'<span class="badge {score_badges[s]}">{score_labels[s]}</span>'
        filing_link = ""
        if fa.get("accession") and fa.get("exhibit_doc"):
            cik_guess = ""
            filing_link = f' &nbsp; <span class="mono" style="color:var(--muted)">{fa["accession"]} / {fa["exhibit_doc"]}</span>'

        programs_html = ""
        if fa["program_mentions"]:
            tags = " ".join(f'<span class="badge badge-blue">{p}</span>' for p in fa["program_mentions"][:8])
            programs_html = f'<div style="margin-top:8px"><strong>Programs / keywords found:</strong> {tags}</div>'

        facility_html = ""
        if fa["facility_mentions"]:
            items = "".join(f'<li style="margin-bottom:6px">{_trunc(_esc(s), 300)}</li>' for s in fa["facility_mentions"][:4])
            facility_html = f'<div style="margin-top:12px"><strong>Facility / manufacturing sentences:</strong><ul style="margin:8px 0 0 16px;line-height:1.7">{items}</ul></div>'

        capex_html = ""
        if fa["capex_sentences"]:
            items = "".join(f'<li style="margin-bottom:6px">{_trunc(_esc(s), 300)}</li>' for s in fa["capex_sentences"][:3])
            capex_html = f'<div style="margin-top:12px"><strong>CapEx / spend sentences:</strong><ul style="margin:8px 0 0 16px;line-height:1.7">{items}</ul></div>'

        parts.append(f"""<div class="evidence" style="margin-bottom:20px">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
  <strong>{fa["quarter"]}</strong> &nbsp; {badge} &nbsp; <span style="color:var(--muted);font-size:11px">filed {fa.get('filing_date','—')}{filing_link}</span>
</div>
<div style="font-size:13px;color:var(--text);margin-bottom:4px"><em>{_esc(fa['summary'])}</em></div>
{programs_html}{facility_html}{capex_html}
</div>""")

    return "".join(parts)


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _conclusion(ticker, name, anomalies, flat_trials, evasive_qa, filing_analysis=None) -> str:
    if not anomalies:
        return f"<p>No anomaly was detected for {name} ({ticker}). The CapEx spike and QFG gap conditions were not simultaneously met in any quarter within the lookback window.</p>"

    filing_analysis = filing_analysis or []
    explained = [fa for fa in filing_analysis if fa["explanation_score"] > 0]
    unexplained = [fa for fa in filing_analysis if fa["explanation_score"] == 0]

    strength = "strong" if (flat_trials or evasive_qa) else "moderate"
    bullets = [f"<li><strong>{a['quarter']}:</strong> CapEx rose {a['qoq_pct']:+.1f}% QoQ with no quantified forward guidance in 8-K filings.</li>" for a in anomalies]

    for fa in explained:
        progs = ", ".join(fa["program_mentions"][:4]) if fa["program_mentions"] else "unspecified program(s)"
        label = "partial" if fa["explanation_score"] == 1 else "explicit"
        bullets.append(f"<li><strong>{fa['quarter']} press release ({label} context):</strong> Facility or manufacturing investment mentioned in context of {progs}.</li>")
    for fa in unexplained:
        if fa["filing_date"]:
            bullets.append(f"<li><strong>{fa['quarter']} press release:</strong> No capex or facility context found despite filing on {fa['filing_date']}.</li>")

    if flat_trials:
        bullets.append(f"<li>{len(flat_trials)} clinical trial(s) show flat or non-recruiting enrollment.</li>")
    if evasive_qa:
        bullets.append(f"<li>{len(evasive_qa)} earnings call instance(s) where management deflected capital allocation questions.</li>")

    next_steps = []
    if explained:
        progs_all = sorted({p for fa in explained for p in fa["program_mentions"][:4]})
        if progs_all:
            next_steps.append(f"Investigate {', '.join(progs_all[:4])} pipeline stage and manufacturing requirements.")
    next_steps.append("Pull 10-Q for the anomaly quarter(s) and read the PP&E footnote for facility details.")
    next_steps.append("Check Form 4 filings for insider activity around the anomaly quarter(s).")
    if flat_trials:
        next_steps.append("Review flat trial enrollment against expected timelines from the company's guidance.")
    ns_html = "".join(f"<li>{s}</li>" for s in next_steps)

    return f"""<p>The evidence presents a <strong>{strength} case</strong> for the anomaly signal in {name} ({ticker}):</p>
<ul style="margin:12px 0 12px 20px;line-height:1.8">{"".join(bullets)}</ul>
<p style="margin-top:16px"><strong>Suggested next steps:</strong></p>
<ul style="margin:8px 0 0 20px;line-height:1.8">{ns_html}</ul>"""


# ── helpers ───────────────────────────────────────────────────────────────────

import re as _re
_EVASION_HL = _re.compile(
    r"\b(don'?t provide|not going to|not in a position|can'?t comment|"
    r"we'?ll discuss|at this time|at a later|premature|nothing to announce|"
    r"nothing to share|not something we|stay tuned|more to come|"
    r"not prepared to|we'?re not|won'?t be|decline to|not ready|"
    r"we haven'?t finalized|under review|being evaluated)\b",
    _re.IGNORECASE,
)


def _highlight_evasion(text: str) -> str:
    safe = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return _EVASION_HL.sub(lambda m: f'<span class="hit">{m.group()}</span>', safe)


def _trunc(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n] + "…"


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


def _continue_research_button(ticker: str, company_name: str, anomalies: list[dict]) -> str:
    if not anomalies:
        return ""
    import json as _json
    quarters     = [a["quarter"] for a in anomalies]
    period_ends  = [a["period_end"].strftime("%Y-%m-%d") for a in anomalies]
    payload = _json.dumps({
        "ticker":               ticker,
        "company_name":         company_name,
        "anomaly_quarters":     quarters,
        "anomaly_period_ends":  period_ends,
    })
    safe_payload = payload.replace("'", "\\'")
    return f"""<div style="margin-top:40px;padding-top:28px;border-top:2px solid #d4a72c">
  <div style="font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#0969da;margin-bottom:10px">Deep Research — Auto-Running</div>
  <div id="deep-research-status" style="font-size:13px;color:#656d76">Fetching Form 4 filings and 10-Q PP&amp;E footnote…</div>
</div>
<script>
(async function runDeepResearch() {{
  const status = document.getElementById('deep-research-status');
  try {{
    const resp = await fetch('/api/deep-research', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({safe_payload}),
    }});
    if (!resp.ok) {{
      const err = await resp.text();
      status.textContent = 'Deep research error: ' + err;
      return;
    }}
    const {{ report_url }} = await resp.json();
    status.innerHTML = '✓ Deep research complete — <a href="' + report_url + '" target="_blank" style="color:#0969da;font-weight:700">Open full report →</a>';
  }} catch (e) {{
    status.textContent = 'Deep research failed: ' + e.message;
  }}
}})();
</script>"""
