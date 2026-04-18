"""
QFG (Quantified Forward Guidance) scraper — scans 8-K filings for the
three-part sentence pattern defined in STATE.md.

For each quarter-end date provided, searches 8-K filings (Items 2.02
and 7.01) filed within [period_end, period_end + window_days] and
returns flag=1 if any sentence contains ALL of:
  1. A currency figure  ($XM / $X million / $X billion)
  2. An expansion noun  (capacity, facility, manufacturing, …)
  3. Future-tense language  (will, plan to, expect to, …)
Returns flag=0 otherwise.
"""

import re
from datetime import datetime, timedelta

from scrapers.edgar import get_cik, get_filing_text, list_filings


def build_qfg_pattern(cfg: dict) -> re.Pattern:
    """Compile the three-part co-occurrence regex from config."""
    currency  = cfg["currency"]
    nouns     = "|".join(re.escape(n) for n in cfg["expansion_nouns"])
    future    = "|".join(re.escape(f) for f in cfg["future_tense"])
    # Each part must appear somewhere in the sentence (order-independent)
    return {
        "currency":  re.compile(currency, re.IGNORECASE),
        "expansion": re.compile(rf'\b({nouns})\b', re.IGNORECASE),
        "future":    re.compile(rf'\b({future})\b', re.IGNORECASE),
    }


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences on . ! ? boundaries."""
    raw = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in raw if len(s.strip()) > 30]


def _check_sentence(sentence: str, patterns: dict) -> bool:
    return (
        patterns["currency"].search(sentence) is not None
        and patterns["expansion"].search(sentence) is not None
        and patterns["future"].search(sentence) is not None
    )


def get_qfg_flags(
    ticker: str,
    period_ends: list[datetime],
    qfg_cfg: dict,
    window_days: int = 75,
) -> dict[datetime, dict]:
    """
    For each period_end in period_ends, scan 8-K filings filed within
    [period_end, period_end + window_days] and compute the QFG flag.

    Returns dict keyed by period_end:
      {
        "flag": 0 or 1,
        "matches": [list of matching sentences],
        "filings_checked": int,
        "filing_dates": [list of 8-K filing dates checked],
      }
    """
    cik      = get_cik(ticker)
    patterns = build_qfg_pattern(qfg_cfg)
    filings  = list_filings(cik, "8-K")

    # Filter to only Items 2.02 / 7.01 filings
    relevant = [
        f for f in filings
        if "2.02" in f["items"] or "7.01" in f["items"]
    ]

    results = {}
    for period_end in period_ends:
        window_start = period_end
        window_end   = period_end + timedelta(days=window_days)

        in_window = [
            f for f in relevant
            if window_start <= datetime.strptime(f["filingDate"], "%Y-%m-%d") <= window_end
        ]

        flag            = 0
        all_matches: list[str] = []
        filing_dates    = [f["filingDate"] for f in in_window]

        for filing in in_window:
            try:
                text      = get_filing_text(cik, filing["accessionNumber"], filing["primaryDocument"])
                sentences = _split_sentences(text)
                matches   = [s for s in sentences if _check_sentence(s, patterns)]
                if matches:
                    flag = 1
                    all_matches.extend(matches)
            except Exception as exc:
                # Network or parse error — log and continue; conservative = flag stays 0
                print(f"    [warn] Could not fetch {filing['accessionNumber']}: {exc}")

        results[period_end] = {
            "flag":             flag,
            "matches":          all_matches,
            "filings_checked":  len(in_window),
            "filing_dates":     filing_dates,
        }

    return results
