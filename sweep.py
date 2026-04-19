#!/usr/bin/env python3
"""
Universe sweep — runs the CapEx + QFG signal across every company in
biotech_universe.yaml and produces a ranked hit list.

Usage:
    python sweep.py                    # all companies
    python sweep.py --limit 10         # first N companies
    python sweep.py --ticker SRPT      # single ticker

Output:
    output/sweep_YYYYMMDD_HHMM.html   — ranked HTML hit list
    stdout                             — live progress + summary table
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import yaml

import logger as _logger_mod
from logger import get_logger
from scrapers.capex_scraper import get_capex_quarterly
from scrapers.qfg_scraper   import get_qfg_flags

_logger_mod.setup()
log = get_logger(__name__)

UNIVERSE_PATH = Path(__file__).parent / "biotech_universe.yaml"
CONFIG_PATH   = Path(__file__).parent / "config.yaml"
OUTPUT_DIR    = Path(__file__).parent / "output"


def main():
    parser = argparse.ArgumentParser(description="Sweep biotech universe for anomaly signal")
    parser.add_argument("--limit",  type=int, default=0,  help="Scan only first N companies")
    parser.add_argument("--ticker", type=str, default="", help="Scan a single ticker only")
    args = parser.parse_args()

    cfg        = yaml.safe_load(CONFIG_PATH.read_text())
    universe   = yaml.safe_load(UNIVERSE_PATH.read_text())
    sprint_cfg = cfg["sprint"]
    thresh     = sprint_cfg["spike_threshold"]
    n          = sprint_cfg["lookback_quarters"]
    window     = sprint_cfg["qfg_window_days_after"]
    min_dt     = datetime.strptime(sprint_cfg["min_date"], "%Y-%m-%d") if sprint_cfg.get("min_date") else None
    tag_default = universe.get("capex_tag_default", "PaymentsToAcquirePropertyPlantAndEquipment")

    companies = universe["companies"]
    if args.ticker:
        companies = [c for c in companies if c["ticker"].upper() == args.ticker.upper()]
        if not companies:
            print(f"Ticker '{args.ticker}' not found in universe.")
            sys.exit(1)
    elif args.limit:
        companies = companies[:args.limit]

    OUTPUT_DIR.mkdir(exist_ok=True)
    total    = len(companies)
    hits     = []
    no_data  = []
    errors   = []

    print(f"\n{'='*65}")
    print(f"  UNIVERSE SWEEP — {total} companies — min_date={sprint_cfg.get('min_date','none')}")
    print(f"{'='*65}\n")

    for idx, company in enumerate(companies, 1):
        ticker = company["ticker"]
        name   = company.get("name", ticker)
        tag    = company.get("capex_tag", tag_default)
        print(f"  [{idx:02d}/{total}] {ticker:<6} {name}")

        cik_override = company.get("cik")
        try:
            capex_df = get_capex_quarterly(ticker, tag, spike_threshold=thresh,
                                           n_quarters=n, min_date=min_dt,
                                           cik_override=cik_override)
            if capex_df.empty:
                print(f"         → no data")
                no_data.append(ticker)
                continue

            spike_quarters = capex_df[capex_df["spike"]]
            if spike_quarters.empty:
                print(f"         → {len(capex_df)} quarters, no spikes")
                continue

            period_ends = capex_df["period_end"].tolist()
            qfg_flags   = get_qfg_flags(ticker, period_ends, cfg["qfg_regex"],
                                         window_days=window, cik_override=cik_override)

            anomalies = []
            for _, row in capex_df.iterrows():
                pe  = row["period_end"]
                qfg = qfg_flags.get(pe, {})
                if row["spike"] and qfg.get("flag") == 0:
                    anomalies.append({
                        "quarter":   _ql(pe),
                        "period_end": pe,
                        "qoq_pct":   row["qoq_pct"],
                        "value_usd": row["value_usd"],
                        "qfg":       qfg,
                    })

            if anomalies:
                print(f"         → ★ ANOMALY in {', '.join(a['quarter'] for a in anomalies)}")
                hits.append({
                    "ticker":    ticker,
                    "name":      name,
                    "notes":     company.get("notes", ""),
                    "cik":       cik_override,
                    "anomalies": anomalies,
                    "capex_df":  capex_df,
                    "qfg":       qfg_flags,
                })
            else:
                best_spike = capex_df["qoq_pct"].max()
                print(f"         → {len(capex_df)} quarters, best spike={best_spike:+.0f}% (QFG covered)")

        except Exception as exc:
            log.error("[%s] sweep error: %s", ticker, exc, exc_info=True)
            print(f"         → ERROR: {exc}")
            errors.append((ticker, str(exc)))

    # ── summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  SWEEP COMPLETE — {len(hits)} anomaly hit(s) of {total} scanned")
    print(f"{'='*65}\n")

    if hits:
        # Flatten anomalies and score by recency + non-Q2 bonus
        all_anomalies = []
        for h in hits:
            for a in h["anomalies"]:
                q_num = int(a["quarter"].split("-Q")[1])
                year  = int(a["quarter"].split("-Q")[0])
                recency_score = year + (q_num / 10)
                q2_penalty    = -0.5 if q_num == 2 else 0  # Q2 spikes are seasonal, down-weight
                score = recency_score + q2_penalty
                all_anomalies.append({**a, "ticker": h["ticker"], "name": h["name"],
                                      "notes": h["notes"], "score": score, "q2": q_num == 2})

        all_anomalies.sort(key=lambda x: x["score"], reverse=True)

        print(f"\n  {'Ticker':<8} {'Quarter':<12} {'QoQ%':>9}  {'Seasonal?':>10}  Name")
        print(f"  {'-'*70}")
        for a in all_anomalies:
            flag = "  (Q2 seasonal)" if a["q2"] else ""
            pct  = f"{a['qoq_pct']:>+7.1f}%" if a["qoq_pct"] != float("inf") else "  +inf%"
            print(f"  {a['ticker']:<8} {a['quarter']:<12} {pct}  {flag}")

        # Highlight highest-conviction (non-Q2, recent)
        priority = [a for a in all_anomalies if not a["q2"] and int(a["quarter"].split("-Q")[0]) >= 2025]
        if priority:
            print(f"\n  ★ PRIORITY TARGETS (2025+, non-Q2):")
            for a in priority:
                pct = f"{a['qoq_pct']:>+7.1f}%" if a["qoq_pct"] != float("inf") else "  +inf%"
                print(f"    {a['ticker']:<8} {a['quarter']}  {pct}  {a['name']}")

    if errors:
        print(f"\n  Errors ({len(errors)}): {', '.join(t for t, _ in errors)}")
    if no_data:
        print(f"  No XBRL data: {', '.join(no_data)}")

    # ── hit list report + ranked JSON ────────────────────────────────────────
    ts          = datetime.now().strftime("%Y%m%d_%H%M")
    report_path = OUTPUT_DIR / f"sweep_{ts}.html"
    _write_sweep_report(hits, no_data, errors, total, thresh, report_path)
    print(f"\n  Sweep report → {report_path}\n")

    # Write ranked JSON for the UI
    ranked = []
    if hits:
        for h in hits:
            for a in h["anomalies"]:
                q_num = int(a["quarter"].split("-Q")[1])
                year  = int(a["quarter"].split("-Q")[0])
                score = year + (q_num / 10) + (-0.5 if q_num == 2 else 0)
                ranked.append({
                    "ticker":     h["ticker"],
                    "name":       h["name"],
                    "notes":      h["notes"],
                    "quarter":    a["quarter"],
                    "period_end": a["period_end"].strftime("%Y-%m-%d"),
                    "qoq_pct":    round(a["qoq_pct"], 1) if a["qoq_pct"] != float("inf") else None,
                    "score":      round(score, 2),
                    "q2":         q_num == 2,
                    "priority":   not (q_num == 2) and year >= 2025,
                    "cik":        h.get("cik"),
                })
        ranked.sort(key=lambda x: x["score"], reverse=True)

    sweep_json = {
        "generated_at": datetime.now().isoformat(),
        "scanned": total,
        "hits": len(hits),
        "ranked": ranked,
        "report_file": report_path.name,
    }
    (OUTPUT_DIR / "sweep_latest.json").write_text(json.dumps(sweep_json, indent=2))


def _write_sweep_report(hits, no_data, errors, total, thresh, path: Path):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    rows = ""
    for h in sorted(hits, key=lambda x: max(a["qoq_pct"] for a in x["anomalies"]), reverse=True):
        for a in h["anomalies"]:
            nct_link = f"https://clinicaltrials.gov/search?spons={h['ticker']}"
            rows += f"""<tr>
<td><strong>{h['ticker']}</strong></td>
<td>{h['name']}</td>
<td>{a['quarter']}</td>
<td style="color:#cf222e;font-weight:700">{a['qoq_pct']:+.1f}%</td>
<td>${a['value_usd']/1e6:.1f}M</td>
<td>{a['qfg'].get('filings_checked',0)} checked, 0 match</td>
<td style="font-size:11px;color:#656d76">{h['notes']}</td>
<td><a href="{nct_link}" target="_blank">CT.gov</a></td>
</tr>"""

    path.write_text(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Sweep Results</title>
<style>
  body{{font-family:-apple-system,sans-serif;font-size:13px;color:#1f2328;padding:32px;max-width:1200px;margin:0 auto}}
  h1{{font-size:22px;margin-bottom:4px}} .meta{{color:#656d76;font-size:12px;margin-bottom:24px}}
  table{{width:100%;border-collapse:collapse}} th{{background:#f6f8fa;text-align:left;padding:8px 10px;
  font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;border-bottom:2px solid #d0d7de;color:#656d76}}
  td{{padding:8px 10px;border-bottom:1px solid #d0d7de;vertical-align:top}}
  tr:hover td{{background:#f6f8fa}}
  .empty{{color:#656d76;font-style:italic;padding:20px 0}}
</style></head><body>
<h1>Biotech Universe Sweep — Anomaly Hit List</h1>
<div class="meta">Generated {ts} &nbsp;·&nbsp; {len(hits)} hit(s) of {total} scanned &nbsp;·&nbsp; Spike threshold ≥{thresh*100:.0f}% &nbsp;·&nbsp; Errors: {len(errors)} &nbsp;·&nbsp; No data: {len(no_data)}</div>
{"<table><thead><tr><th>Ticker</th><th>Company</th><th>Quarter</th><th>QoQ%</th><th>CapEx</th><th>QFG Scan</th><th>Notes</th><th>Links</th></tr></thead><tbody>" + rows + "</tbody></table>" if hits else '<p class="empty">No anomaly hits found in this sweep.</p>'}
</body></html>""", encoding="utf-8")


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


if __name__ == "__main__":
    main()
