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

from logger import get_logger
from scrapers.edgar import get_cik, get_filing_text, list_filings

log = get_logger(__name__)


def build_qfg_pattern(cfg: dict) -> dict:
    """Compile the three-part co-occurrence regex from config."""
    currency  = cfg["currency"]
    nouns     = "|".join(re.escape(n) for n in cfg["expansion_nouns"])
    future    = "|".join(re.escape(f) for f in cfg["future_tense"])
    patterns = {
        "currency":  re.compile(currency, re.IGNORECASE),
        "expansion": re.compile(rf'\b({nouns})\b', re.IGNORECASE),
        "future":    re.compile(rf'\b({future})\b', re.IGNORECASE),
    }
    log.debug(
        "QFG patterns compiled — currency: %s | expansion nouns: %s | future: %s",
        currency, nouns, future,
    )
    return patterns


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
    cik_override: str | None = None,
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
    log.info("[%s] Starting QFG scan for %d quarters (window: +%d days)", ticker, len(period_ends), window_days)
    cik      = cik_override if cik_override else get_cik(ticker)
    patterns = build_qfg_pattern(qfg_cfg)
    filings  = list_filings(cik, "8-K")

    # Filter to only Items 2.02 / 7.01 filings
    relevant = [
        f for f in filings
        if "2.02" in f["items"] or "7.01" in f["items"]
    ]
    log.debug("[%s] 8-K filings with Item 2.02 or 7.01: %d of %d total", ticker, len(relevant), len(filings))

    results = {}
    for period_end in period_ends:
        window_start = period_end
        window_end   = period_end + timedelta(days=window_days)
        period_label = _ql(period_end)

        in_window = [
            f for f in relevant
            if window_start <= datetime.strptime(f["filingDate"], "%Y-%m-%d") <= window_end
        ]
        log.debug(
            "[%s] %s: %d 8-K(s) in window [%s → %s]",
            ticker, period_label, len(in_window),
            window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d"),
        )

        flag: int           = 0
        all_matches: list[str] = []
        filing_dates        = [f["filingDate"] for f in in_window]
        fetch_errors        = 0

        for filing in in_window:
            accn     = filing["accessionNumber"]
            doc      = filing["primaryDocument"]
            filed_on = filing["filingDate"]
            try:
                text      = get_filing_text(cik, accn, doc)
                sentences = _split_sentences(text)
                matches   = [s for s in sentences if _check_sentence(s, patterns)]
                log.debug(
                    "[%s] %s filing %s (%s): %d sentences, %d match(es)",
                    ticker, period_label, accn, filed_on, len(sentences), len(matches),
                )
                if matches:
                    flag = 1
                    all_matches.extend(matches)
                    log.info(
                        "[%s] %s QFG=1 — match in filing %s filed %s",
                        ticker, period_label, accn, filed_on,
                    )
                    for m in matches:
                        log.debug("  Matched sentence: %.120s…", m)
            except Exception:
                fetch_errors += 1
                log.warning(
                    "[%s] %s — failed to fetch/parse filing %s (%s); skipping (flag stays 0)",
                    ticker, period_label, accn, doc, exc_info=True,
                )

        if fetch_errors:
            log.warning(
                "[%s] %s — %d of %d filing(s) could not be fetched; QFG result may be under-counted",
                ticker, period_label, fetch_errors, len(in_window),
            )

        if flag == 0 and in_window:
            log.info("[%s] %s QFG=0 — no qualifying sentence in %d filing(s)", ticker, period_label, len(in_window))
        elif not in_window:
            log.info("[%s] %s QFG=0 — no 8-K Item 2.02/7.01 filings found in window", ticker, period_label)

        results[period_end] = {
            "flag":             flag,
            "matches":          all_matches,
            "filings_checked":  len(in_window),
            "filing_dates":     filing_dates,
        }

    return results


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"
