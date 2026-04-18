"""
CapEx scraper — pulls quarterly capital expenditure from EDGAR XBRL.

Uses the XBRL company facts API to extract a specific US-GAAP tag
(e.g. PaymentsToAcquirePropertyPlantAndEquipment) and returns a
DataFrame of single-quarter values with QoQ % change and spike flag.
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
) -> pd.DataFrame:
    """
    Return DataFrame with columns:
      period_end, period_start, value_usd, qoq_pct, spike
    Sorted ascending by period_end, limited to n_quarters most recent rows.
    If min_date is set, only quarters ending on or after that date are kept.
    Returns empty DataFrame if the tag is not found or has insufficient data.
    """
    log.info("[%s] Fetching CapEx via XBRL tag '%s'", ticker, xbrl_tag)
    cik = get_cik(ticker)
    facts = get_company_facts(cik)

    tag_data = (
        facts
        .get("facts", {})
        .get("us-gaap", {})
        .get(xbrl_tag, {})
    )
    usd_entries = tag_data.get("units", {}).get("USD", [])

    if not usd_entries:
        log.debug("[%s] Tag '%s' not in us-gaap; trying dei namespace", ticker, xbrl_tag)
        tag_data = (
            facts
            .get("facts", {})
            .get("dei", {})
            .get(xbrl_tag, {})
        )
        usd_entries = tag_data.get("units", {}).get("USD", [])

    if not usd_entries:
        log.warning(
            "[%s] XBRL tag '%s' not found in us-gaap or dei namespaces — "
            "check the tag name in config.yaml",
            ticker, xbrl_tag,
        )
        return pd.DataFrame(
            columns=["period_end", "period_start", "value_usd", "qoq_pct", "spike"]
        )

    log.debug("[%s] Raw XBRL entries for tag: %d", ticker, len(usd_entries))

    rows = []
    seen_ends = set()
    skipped_period = 0

    for e in usd_entries:
        start_str = e.get("start")
        end_str   = e.get("end")
        if not start_str or not end_str:
            continue
        try:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            end_dt   = datetime.strptime(end_str,   "%Y-%m-%d")
        except ValueError:
            log.warning("[%s] Unparseable date pair: start=%s end=%s", ticker, start_str, end_str)
            continue

        # Keep only single-quarter records (~90 day periods)
        days = (end_dt - start_dt).days
        if not (80 <= days <= 100):
            skipped_period += 1
            continue

        # Deduplicate: if multiple filings cover same period, keep latest filed
        if end_dt in seen_ends:
            existing_idx = next(
                (i for i, r in enumerate(rows) if r["period_end"] == end_dt), None
            )
            if existing_idx is not None:
                existing_filed = rows[existing_idx].get("filed", "")
                new_filed      = e.get("filed", "")
                if new_filed > existing_filed:
                    log.debug(
                        "[%s] Replacing duplicate period %s with newer filing %s",
                        ticker, end_str, new_filed,
                    )
                    rows[existing_idx] = _row(e, start_dt, end_dt)
            continue

        if min_date and end_dt < min_date:
            continue

        seen_ends.add(end_dt)
        rows.append(_row(e, start_dt, end_dt))

    log.debug(
        "[%s] XBRL filtering: %d quarterly rows kept, %d non-quarterly skipped",
        ticker, len(rows), skipped_period,
    )

    if not rows:
        log.warning(
            "[%s] No single-quarter (~90-day) entries found for tag '%s'",
            ticker, xbrl_tag,
        )
        return pd.DataFrame(
            columns=["period_end", "period_start", "value_usd", "qoq_pct", "spike"]
        )

    df = (
        pd.DataFrame(rows)
        .sort_values("period_end")
        .tail(n_quarters)
        .reset_index(drop=True)
    )

    df["qoq_pct"] = df["value_usd"].pct_change() * 100
    df["spike"]   = df["qoq_pct"] >= (spike_threshold * 100)

    spike_count = int(df["spike"].sum())
    log.info(
        "[%s] CapEx data ready: %d quarters, %d spike(s) ≥%.0f%%",
        ticker, len(df), spike_count, spike_threshold * 100,
    )

    return df


def _row(entry: dict, start_dt: datetime, end_dt: datetime) -> dict:
    return {
        "period_end":   end_dt,
        "period_start": start_dt,
        "value_usd":    entry.get("val", 0),
        "filed":        entry.get("filed", ""),
        "accn":         entry.get("accn", ""),
    }
