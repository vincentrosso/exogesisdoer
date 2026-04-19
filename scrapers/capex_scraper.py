"""
CapEx scraper — pulls quarterly capital expenditure from EDGAR XBRL.

Handles two reporting styles:
  1. Single-quarter entries (~90-day periods) — used directly.
  2. YTD cumulative entries (6M, 9M, 12M) — quarterly values derived
     by differencing consecutive periods within the same fiscal year.
     This covers companies like ACAD and NVAX that report cumulatively.
"""

from datetime import datetime

import pandas as pd

from logger import get_logger
from scrapers.edgar import get_cik, get_company_facts

log = get_logger(__name__)


def get_capex_quarterly(
    ticker: str,
    xbrl_tag: str,
    spike_threshold: float = 0.40,
    n_quarters: int = 8,
    min_date: datetime | None = None,
    cik_override: str | None = None,
) -> pd.DataFrame:
    """
    Return DataFrame with columns:
      period_end, period_start, value_usd, qoq_pct, spike
    Sorted ascending by period_end, limited to n_quarters most recent rows.
    If min_date is set, only quarters ending on or after that date are kept.
    Accepts cik_override for tickers not in company_tickers.json.
    Returns empty DataFrame if tag not found or insufficient data.
    """
    log.info("[%s] Fetching CapEx via XBRL tag '%s'", ticker, xbrl_tag)
    cik = cik_override if cik_override else get_cik(ticker)
    facts = get_company_facts(cik)

    tag_data    = facts.get("facts", {}).get("us-gaap", {}).get(xbrl_tag, {})
    usd_entries = tag_data.get("units", {}).get("USD", [])

    if not usd_entries:
        log.debug("[%s] Tag '%s' not in us-gaap; trying dei namespace", ticker, xbrl_tag)
        tag_data    = facts.get("facts", {}).get("dei", {}).get(xbrl_tag, {})
        usd_entries = tag_data.get("units", {}).get("USD", [])

    if not usd_entries:
        log.warning("[%s] XBRL tag '%s' not found — check tag name in config/universe", ticker, xbrl_tag)
        return _empty()

    log.debug("[%s] Raw XBRL entries for tag: %d", ticker, len(usd_entries))

    # Bucket entries by period length
    single_q: list[dict] = []   # ~90 day — use directly
    ytd:      list[dict] = []   # 150–380 day — derive quarters from these

    for e in usd_entries:
        start_str = e.get("start")
        end_str   = e.get("end")
        if not start_str or not end_str:
            continue
        try:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            end_dt   = datetime.strptime(end_str,   "%Y-%m-%d")
        except ValueError:
            log.warning("[%s] Unparseable dates: %s / %s", ticker, start_str, end_str)
            continue

        days = (end_dt - start_dt).days
        if 80 <= days <= 100:
            single_q.append({**e, "_start_dt": start_dt, "_end_dt": end_dt, "_days": days})
        elif 150 <= days <= 380:
            ytd.append({**e, "_start_dt": start_dt, "_end_dt": end_dt, "_days": days})

    log.debug("[%s] Bucketed: %d single-quarter, %d YTD", ticker, len(single_q), len(ytd))

    rows_direct = _build_rows_from_single(single_q, ticker)
    rows_ytd    = _derive_from_ytd(ytd, ticker) if ytd else []

    # Merge: direct single-quarter entries take priority; YTD fills any gaps
    direct_ends = {r["period_end"] for r in rows_direct}
    rows = rows_direct + [r for r in rows_ytd if r["period_end"] not in direct_ends]
    log.debug("[%s] Merged: %d direct + %d YTD-derived = %d rows",
              ticker, len(rows_direct), len(rows_ytd) - len(direct_ends & {r["period_end"] for r in rows_ytd}), len(rows))

    if not rows:
        log.warning("[%s] No quarterly data found for tag '%s' (tried direct + YTD)", ticker, xbrl_tag)
        return _empty()

    df = (
        pd.DataFrame(rows)
        .sort_values("period_end")
    )

    if min_date:
        df = df[df["period_end"] >= min_date]

    df = df.tail(n_quarters).reset_index(drop=True)

    if df.empty:
        log.warning("[%s] All quarters filtered out by min_date=%s", ticker, min_date)
        return _empty()

    df["qoq_pct"] = df["value_usd"].pct_change() * 100
    df["spike"]   = df["qoq_pct"] >= (spike_threshold * 100)

    log.info("[%s] CapEx ready: %d quarters, %d spike(s) ≥%.0f%%",
             ticker, len(df), int(df["spike"].sum()), spike_threshold * 100)
    return df


# ── row builders ──────────────────────────────────────────────────────────────

def _build_rows_from_single(entries: list[dict], ticker: str) -> list[dict]:
    """Deduplicate and return rows from direct single-quarter entries."""
    by_end: dict[datetime, dict] = {}
    for e in entries:
        end_dt = e["_end_dt"]
        if end_dt not in by_end or e.get("filed", "") > by_end[end_dt].get("filed", ""):
            by_end[end_dt] = e
    rows = [_row(e, e["_start_dt"], e["_end_dt"]) for e in by_end.values()]
    log.debug("[%s] Single-quarter rows after dedup: %d", ticker, len(rows))
    return rows


def _derive_from_ytd(entries: list[dict], ticker: str) -> list[dict]:
    """
    Derive single-quarter values from YTD cumulative entries.
    Groups by fiscal-year start, then differences consecutive periods.

    Example: if 6M=10M and 3M=4M → Q2 = 6M - 3M = 6M.
    """
    # Keep most-recently-filed version of each (start, end) pair
    by_period: dict[tuple, dict] = {}
    for e in entries:
        key = (e["_start_dt"], e["_end_dt"])
        if key not in by_period or e.get("filed","") > by_period[key].get("filed",""):
            by_period[key] = e

    # Group by fiscal-year start date
    by_fy: dict[datetime, list[dict]] = {}
    for e in by_period.values():
        fy_start = e["_start_dt"]
        by_fy.setdefault(fy_start, []).append(e)

    rows = []
    for fy_start, fy_entries in by_fy.items():
        # Sort ascending by period end (shortest period first)
        fy_entries.sort(key=lambda x: x["_end_dt"])
        prev_val = 0
        prev_end = fy_start

        for e in fy_entries:
            quarter_val = e["val"] - prev_val
            if quarter_val < 0:
                # Can happen with amended filings — skip
                log.debug("[%s] Negative derived quarter (%s→%s): %.0f - %.0f = %.0f; skipping",
                          ticker, prev_end.date(), e["_end_dt"].date(), e["val"], prev_val, quarter_val)
                prev_val = e["val"]
                prev_end = e["_end_dt"]
                continue

            rows.append({
                "period_end":   e["_end_dt"],
                "period_start": prev_end,
                "value_usd":    quarter_val,
                "filed":        e.get("filed", ""),
                "accn":         e.get("accn", ""),
            })
            prev_val = e["val"]
            prev_end = e["_end_dt"]

    log.debug("[%s] YTD derivation produced %d quarterly rows", ticker, len(rows))
    return rows


def _row(entry: dict, start_dt: datetime, end_dt: datetime) -> dict:
    return {
        "period_end":   end_dt,
        "period_start": start_dt,
        "value_usd":    entry.get("val", 0),
        "filed":        entry.get("filed", ""),
        "accn":         entry.get("accn", ""),
    }


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=["period_end", "period_start", "value_usd", "qoq_pct", "spike"])
