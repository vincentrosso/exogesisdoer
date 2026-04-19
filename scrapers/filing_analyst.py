"""
Filing analyst — reads Exhibit 99.1 press releases for anomaly quarters
and extracts capex context: what facility, for which program, and how
explicitly management disclosed it.

Returns a per-quarter dict:
  {
    "quarter":        "2025-Q4",
    "filing_date":    "2026-02-12",
    "accession":      "...",
    "facility_mentions":  [list of sentences mentioning facility/manufacturing],
    "program_mentions":   [list of program names found near facility context],
    "capex_sentences":    [sentences that mention capex/spend/investment],
    "explanation_score":  0 (none) | 1 (partial) | 2 (explicit with $ amount),
    "summary":            "one-line synthesis",
  }
"""

import re
from datetime import datetime, timedelta

from logger import get_logger
from scrapers.edgar import get_cik, get_filing_text, list_filing_documents, list_filings

log = get_logger(__name__)

# Facility / manufacturing context
_FACILITY_RE = re.compile(
    r'\b(facility|facilities|manufacturing|GMP|plant|site|build.?out|'
    r'construction|infrastructure|capacity|square.?feet|sq\.?\s*ft)\b',
    re.IGNORECASE,
)

# Capex spend language
_CAPEX_RE = re.compile(
    r'\b(capital expenditure|capex|property.{0,15}equipment|'
    r'invest(?:ment|ing|ed)|spend(?:ing)?|expenditure)\b',
    re.IGNORECASE,
)

# Currency amounts
_CURRENCY_RE = re.compile(
    r'\$[\d,]+\.?\d*\s*(?:million|billion|[MBK])\b',
    re.IGNORECASE,
)

# Program/asset names: CTX\d+, drug names, pipeline keywords
_PROGRAM_RE = re.compile(
    r'\b(CTX\d+[™®]?|[A-Z]{3,}-(?:cel|mab|nib|mib|zumab|tinib)|'
    r'(?:Phase [123]|pivotal|registrational|IND|NDA|BLA|sNDA)\b|'
    r'allogeneic|autologous|CAR.?T|gene.?editing|cell.?therapy|'
    r'in.?vivo|ex.?vivo|LNP|mRNA|siRNA|CRISPR)\b',
    re.IGNORECASE,
)

# Explanation quality: explicit dollar + facility + forward = score 2
_EXPLICIT_SPEND_RE = re.compile(
    r'\$[\d,]+\.?\d*\s*(?:million|billion|[MBK]).*?(?:facility|manufacturing|capex|infrastructure)',
    re.IGNORECASE | re.DOTALL,
)


def get_filing_analysis(
    ticker: str,
    anomaly_period_ends: list[datetime],
    window_days: int = 90,
    cik_override: str | None = None,
) -> list[dict]:
    """
    For each anomaly quarter, fetch the EX-99.1 press release from the
    closest earnings 8-K and extract context explaining the capex spike.
    """
    if not anomaly_period_ends:
        return []

    log.info("[%s] Filing analysis for %d anomaly quarter(s)", ticker, len(anomaly_period_ends))
    cik     = cik_override if cik_override else get_cik(ticker)
    filings = list_filings(cik, "8-K")
    relevant = [f for f in filings if "2.02" in f["items"] or "7.01" in f["items"]]

    results = []
    for period_end in anomaly_period_ends:
        window_end = period_end + timedelta(days=window_days)
        quarter    = _ql(period_end)

        in_window = [
            f for f in relevant
            if period_end <= datetime.strptime(f["filingDate"], "%Y-%m-%d") <= window_end
        ]
        if not in_window:
            log.info("[%s] %s — no earnings 8-K in window for filing analysis", ticker, quarter)
            results.append(_empty_result(quarter, period_end))
            continue

        filing   = in_window[0]
        accn     = filing["accessionNumber"]
        filed_on = filing["filingDate"]

        # Find the press release exhibit
        press_release_text = ""
        ex_doc = None
        for fdoc in list_filing_documents(cik, accn):
            if fdoc["type"].startswith("EX-99"):
                ex_doc = fdoc["filename"]
                try:
                    press_release_text = get_filing_text(cik, accn, ex_doc)
                    log.info("[%s] %s — fetched %s (%d chars)", ticker, quarter, ex_doc, len(press_release_text))
                except Exception:
                    log.warning("[%s] %s — failed to fetch exhibit %s", ticker, quarter, ex_doc, exc_info=True)
                break

        if not press_release_text:
            # Fall back to primary document
            try:
                press_release_text = get_filing_text(cik, accn, filing["primaryDocument"])
            except Exception:
                log.warning("[%s] %s — failed to fetch primary doc", ticker, quarter, exc_info=True)

        sentences = _split_sentences(press_release_text)

        facility_mentions = [s for s in sentences if _FACILITY_RE.search(s)]
        capex_sentences   = [s for s in sentences if _CAPEX_RE.search(s)]
        program_mentions  = _extract_programs(facility_mentions + capex_sentences)

        # Score explanation quality
        score = 0
        if facility_mentions:
            score = 1
        if any(_CURRENCY_RE.search(s) and _FACILITY_RE.search(s) for s in sentences):
            score = 2

        summary = _synthesize(ticker, quarter, facility_mentions, program_mentions, score)

        log.info(
            "[%s] %s — score=%d, %d facility mentions, %d program refs, summary: %s",
            ticker, quarter, score, len(facility_mentions), len(program_mentions), summary,
        )

        results.append({
            "quarter":           quarter,
            "period_end":        period_end,
            "filing_date":       filed_on,
            "accession":         accn,
            "exhibit_doc":       ex_doc,
            "facility_mentions": facility_mentions[:6],
            "capex_sentences":   capex_sentences[:4],
            "program_mentions":  program_mentions,
            "explanation_score": score,
            "summary":           summary,
        })

    return results


def _split_sentences(text: str) -> list[str]:
    raw = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in raw if len(s.strip()) > 40]


def _extract_programs(sentences: list[str]) -> list[str]:
    found = set()
    for s in sentences:
        for m in _PROGRAM_RE.finditer(s):
            found.add(m.group(0))
    return sorted(found)


def _synthesize(ticker: str, quarter: str, facility_mentions: list[str],
                programs: list[str], score: int) -> str:
    if score == 0:
        return "No facility or capex context found in press release."
    prog_str = ", ".join(programs[:4]) if programs else "unspecified program(s)"
    if score == 2:
        # Pull the explicit dollar sentence
        for s in facility_mentions:
            if _CURRENCY_RE.search(s):
                return s[:200]
    # Score 1 — partial
    if facility_mentions:
        return facility_mentions[0][:200]
    return f"Facility mentioned in context of: {prog_str}"


def _empty_result(quarter: str, period_end: datetime) -> dict:
    return {
        "quarter":           quarter,
        "period_end":        period_end,
        "filing_date":       None,
        "accession":         None,
        "exhibit_doc":       None,
        "facility_mentions": [],
        "capex_sentences":   [],
        "program_mentions":  [],
        "explanation_score": 0,
        "summary":           "No earnings 8-K found in window.",
    }


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"
