#!/usr/bin/env python3
"""
Sprint orchestrator.

Usage:
    python run.py                   # uses config.yaml
    python run.py --force-pivot     # skip primary, go straight to pivot target

Flow:
    1. Run CapEx + QFG scrapers for primary target and peers
    2. Generate dashboard (output/dashboard.html)
    3. Print anomaly analysis table
    4. Decide: PROCEED (anomaly found) or PIVOT (run pivot target)
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import yaml

from scrapers.capex_scraper import get_capex_quarterly
from scrapers.qfg_scraper   import get_qfg_flags
from dashboard.plot         import generate_dashboard

CONFIG_PATH = Path(__file__).parent / "config.yaml"
OUTPUT_DIR  = Path(__file__).parent / "output"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def analyse_company(cfg: dict, company: dict, sprint_cfg: dict) -> dict:
    ticker  = company["ticker"]
    name    = company["name"]
    tag     = company["capex_tag"]
    n       = sprint_cfg["lookback_quarters"]
    thresh  = sprint_cfg["spike_threshold"]
    window  = sprint_cfg["qfg_window_days_after"]

    print(f"\n  [{ticker}] Pulling CapEx XBRL ({tag})…")
    capex_df = get_capex_quarterly(ticker, tag, spike_threshold=thresh, n_quarters=n)

    if capex_df.empty:
        print(f"  [{ticker}] WARNING: No quarterly CapEx data found for tag '{tag}'")
        return {"name": name, "capex": capex_df, "qfg": {}, "anomalies": []}

    period_ends = capex_df["period_end"].tolist()
    print(f"  [{ticker}] Got {len(capex_df)} quarters. Scanning {len(period_ends)} 8-K windows…")

    qfg_flags = get_qfg_flags(
        ticker,
        period_ends,
        qfg_cfg=cfg["qfg_regex"],
        window_days=window,
    )

    # Identify anomaly quarters
    anomalies = []
    for _, row in capex_df.iterrows():
        pe  = row["period_end"]
        qfg = qfg_flags.get(pe, {})
        if row["spike"] and qfg.get("flag") == 0:
            anomalies.append({
                "quarter":    _ql(pe),
                "period_end": pe,
                "qoq_pct":    row["qoq_pct"],
                "value_usd":  row["value_usd"],
                "qfg":        qfg,
            })

    return {
        "name":      name,
        "capex":     capex_df,
        "qfg":       qfg_flags,
        "anomalies": anomalies,
    }


def print_table(ticker: str, entry: dict, thresh: float) -> None:
    df      = entry["capex"]
    qfg_map = entry["qfg"]

    if df.empty:
        print(f"\n  No data to display for {ticker}.")
        return

    header = f"\n  {'Quarter':<12} {'CapEx ($M)':>12} {'QoQ%':>10} {'Spike':>7} {'QFG':>5} {'ANOMALY':>10}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    for _, row in df.iterrows():
        pe     = row["period_end"]
        qfg    = qfg_map.get(pe, {})
        flag   = qfg.get("flag", "?")
        spike  = "YES" if row["spike"] else "no"
        qoq    = f"{row['qoq_pct']:+.1f}%" if row["qoq_pct"] == row["qoq_pct"] else "N/A"
        val_m  = f"{row['value_usd'] / 1_000_000:.1f}"
        star   = "*** ANOMALY ***" if (row["spike"] and flag == 0) else ""
        print(
            f"  {_ql(pe):<12} {val_m:>12} {qoq:>10} {spike:>7} {str(flag):>5} {star:>10}"
        )


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


def run_batch(cfg: dict, companies: list[dict], label: str) -> dict[str, dict]:
    sprint_cfg = cfg["sprint"]
    results    = {}

    print(f"\n{'='*60}")
    print(f"  RUNNING: {label}")
    print(f"{'='*60}")

    for company in companies:
        ticker = company["ticker"]
        try:
            results[ticker] = analyse_company(cfg, company, sprint_cfg)
        except Exception as exc:
            print(f"  [{ticker}] ERROR: {exc}")
            results[ticker] = {
                "name":      company["name"],
                "capex":     __import__("pandas").DataFrame(),
                "qfg":       {},
                "anomalies": [],
            }

    return results


def main():
    parser = argparse.ArgumentParser(description="Sprint anomaly scanner")
    parser.add_argument("--force-pivot", action="store_true", help="Skip primary target, run pivot directly")
    args = parser.parse_args()

    cfg        = load_config()
    sprint_cfg = cfg["sprint"]
    thresh     = sprint_cfg["spike_threshold"]

    OUTPUT_DIR.mkdir(exist_ok=True)

    # ------------------------------------------------------------------ #
    # Phase 1: Primary target + peers                                      #
    # ------------------------------------------------------------------ #
    if not args.force_pivot:
        primary_companies = [cfg["primary"]] + cfg.get("peers", [])
        primary_results   = run_batch(cfg, primary_companies, "PRIMARY BIOTECH TARGET + PEERS")

        print(f"\n{'='*60}")
        print("  ANALYSIS RESULTS")
        print(f"{'='*60}")

        for ticker, entry in primary_results.items():
            print(f"\n  {entry['name']} ({ticker})")
            print_table(ticker, entry, thresh)

        # Check for anomalies in primary target only
        primary_ticker    = cfg["primary"]["ticker"]
        primary_entry     = primary_results.get(primary_ticker, {})
        primary_anomalies = primary_entry.get("anomalies", [])

        # Generate primary dashboard
        dash_path = OUTPUT_DIR / "dashboard_primary.html"
        try:
            generate_dashboard(
                {t: {"name": e["name"], "capex": e["capex"], "qfg": e["qfg"]}
                 for t, e in primary_results.items()},
                spike_threshold=thresh,
                output_path=dash_path,
            )
            print(f"\n  Dashboard saved → {dash_path}")
        except Exception as exc:
            print(f"\n  [warn] Dashboard generation failed: {exc}")

        print(f"\n{'='*60}")
        if primary_anomalies:
            print(f"  VERDICT: ANOMALY FOUND in {primary_ticker}")
            print(f"  ACTION:  PROCEED to secondary work")
            print(f"{'='*60}")
            print("\n  Anomaly quarters:")
            for a in primary_anomalies:
                print(f"    • {a['quarter']}  QoQ={a['qoq_pct']:+.1f}%  QFG=0")
                if a["qfg"].get("filing_dates"):
                    print(f"      8-Ks checked: {', '.join(a['qfg']['filing_dates'])}")
            print("\n  NEXT STEPS (Hours 12-48):")
            print("    1. ClinicalTrials.gov — confirm flat enrollment for target trials")
            print("    2. Earnings call Q&A  — scan for evasive capital allocation answers")
            print("    3. Build spectacle report narrative")
            return 0
        else:
            print(f"  VERDICT: No anomaly found in {primary_ticker}")
            print(f"  ACTION:  PIVOTING to software target")
            print(f"{'='*60}")

    # ------------------------------------------------------------------ #
    # Phase 2: Pivot target                                                #
    # ------------------------------------------------------------------ #
    pivot_cfg     = cfg["pivot"]
    pivot_results = run_batch(cfg, [pivot_cfg], f"PIVOT TARGET: {pivot_cfg['name']} ({pivot_cfg['ticker']})")

    print(f"\n{'='*60}")
    print("  PIVOT ANALYSIS RESULTS")
    print(f"{'='*60}")

    pivot_ticker  = pivot_cfg["ticker"]
    pivot_entry   = pivot_results.get(pivot_ticker, {})
    print(f"\n  {pivot_entry.get('name', pivot_ticker)} ({pivot_ticker})")
    print_table(pivot_ticker, pivot_entry, thresh)

    pivot_anomalies = pivot_entry.get("anomalies", [])

    dash_path = OUTPUT_DIR / "dashboard_pivot.html"
    try:
        generate_dashboard(
            {t: {"name": e["name"], "capex": e["capex"], "qfg": e["qfg"]}
             for t, e in pivot_results.items()},
            spike_threshold=thresh,
            output_path=dash_path,
        )
        print(f"\n  Dashboard saved → {dash_path}")
    except Exception as exc:
        print(f"\n  [warn] Dashboard generation failed: {exc}")

    print(f"\n{'='*60}")
    if pivot_anomalies:
        print(f"  VERDICT: ANOMALY FOUND in {pivot_ticker}")
        print(f"  ACTION:  PROCEED to secondary work (pivot target)")
        print(f"{'='*60}")
        for a in pivot_anomalies:
            print(f"    • {a['quarter']}  QoQ={a['qoq_pct']:+.1f}%  QFG=0")
    else:
        print(f"  VERDICT: No anomaly found in either target")
        print(f"  ACTION:  Sprint concludes with null result")
        print(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
