"""
10-Q PP&E footnote scraper.

Finds the 10-Q filing for an anomaly quarter and extracts:
  - Property, plant & equipment footnote text
  - Construction in progress line items
  - Capital commitments / operating lease commitments
  - Any facility or manufacturing-specific descriptions
"""

import re
from datetime import datetime, timedelta

from logger import get_logger
from scrapers.edgar import get_cik, get_filing_text, list_filing_documents, list_filings

log = get_logger(__name__)

_PPE_HEADER_RE = re.compile(
    r'(?:property,?\s*plant\s*(?:and|&)\s*equipment|'
    r'PP&?E\b|capital\s*expenditure|construction\s*in\s*progress)',
    re.IGNORECASE,
)

_FACILITY_RE = re.compile(
    r'\b(facility|facilities|manufacturing|lab(?:oratory)?|'
    r'building|leasehold|construction|GMP|clean.?room|'
    r'Framingham|Boston|Cambridge|San\s+Francisco|Zug|'
    r'square\s*f(?:eet|t)|sq\.?\s*ft)\b',
    re.IGNORECASE,
)

_COMMITMENT_RE = re.compile(
    r'\b(commit(?:ment|ted)|obligat(?:ion|ed)|'
    r'future\s+(?:minimum\s+)?(?:payment|lease|rent)|'
    r'capital\s+(?:lease|commit))\b',
    re.IGNORECASE,
)

_DOLLAR_RE = re.compile(r'\$[\d,]+\.?\d*\s*(?:million|billion|thousand|[MBK])?', re.IGNORECASE)


def get_ppe_analysis(
    ticker: str,
    anomaly_period_ends: list[datetime],
    cik_override: str | None = None,
) -> list[dict]:
    """
    For each anomaly quarter, find the 10-Q (or 10-K for Q4) and extract
    PP&E footnote content, CIP line items, and capital commitments.

    Returns list of dicts per quarter:
      {quarter, filing_date, accession, form_type,
       ppe_sentences, cip_items, commitment_sentences,
       facility_descriptions, dollar_amounts, summary}
    """
    if not anomaly_period_ends:
        return []

    log.info("[%s] PP&E analysis for %d anomaly quarter(s)", ticker, len(anomaly_period_ends))
    cik      = cik_override if cik_override else get_cik(ticker)
    tenqs    = list_filings(cik, "10-Q")
    tenks    = list_filings(cik, "10-K")
    all_ann  = sorted(tenqs + tenks, key=lambda x: x["filingDate"])

    results = []
    for period_end in anomaly_period_ends:
        quarter    = _ql(period_end)
        # Look for 10-Q/10-K filed within 75 days after quarter end
        window_end = period_end + timedelta(days=75)

        candidates = [
            f for f in all_ann
            if period_end <= datetime.strptime(f["filingDate"], "%Y-%m-%d") <= window_end
        ]
        if not candidates:
            log.info("[%s] %s — no 10-Q/10-K found in window", ticker, quarter)
            results.append(_empty(quarter, period_end))
            continue

        filing   = candidates[0]
        accn     = filing["accessionNumber"]
        filed_on = filing["filingDate"]
        form     = filing.get("form", "10-Q")
        log.info("[%s] %s — using %s filed %s (%s)", ticker, quarter, form, filed_on, accn)

        # Try to find the right document: prefer the main filing doc
        text = ""
        docs = list_filing_documents(cik, accn)
        primary = filing["primaryDocument"]
        # Look for main 10-Q/10-K htm (often largest file)
        for d in docs:
            if d["type"] in ("10-Q", "10-K") and d["filename"].endswith((".htm", ".html")):
                primary = d["filename"]
                break

        try:
            text = get_filing_text(cik, accn, primary)
            log.debug("[%s] %s — fetched %s (%d chars)", ticker, quarter, primary, len(text))
        except Exception:
            log.warning("[%s] %s — failed to fetch %s", ticker, quarter, primary, exc_info=True)
            results.append(_empty(quarter, period_end))
            continue

        sentences = _split_sentences(text)

        ppe_sentences     = [s for s in sentences if _PPE_HEADER_RE.search(s)]
        facility_sents    = [s for s in sentences if _FACILITY_RE.search(s)]
        commitment_sents  = [s for s in sentences if _COMMITMENT_RE.search(s)]
        cip_items         = [s for s in sentences if re.search(r'construction.in.progress|CIP\b', s, re.IGNORECASE)]
        dollar_amounts    = _extract_dollar_context(ppe_sentences + facility_sents + cip_items)

        summary = _summarize(quarter, ppe_sentences, facility_sents, cip_items, dollar_amounts)

        log.info(
            "[%s] %s — %d PP&E sentences, %d facility, %d CIP, %d commitments",
            ticker, quarter, len(ppe_sentences), len(facility_sents), len(cip_items), len(commitment_sents),
        )

        results.append({
            "quarter":              quarter,
            "period_end":           period_end,
            "filing_date":          filed_on,
            "accession":            accn,
            "form_type":            form,
            "ppe_sentences":        ppe_sentences[:6],
            "cip_items":            cip_items[:4],
            "commitment_sentences": commitment_sents[:4],
            "facility_descriptions": facility_sents[:6],
            "dollar_amounts":       dollar_amounts,
            "summary":              summary,
        })

    return results


def _split_sentences(text: str) -> list[str]:
    raw = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in raw if 30 < len(s.strip()) < 800]


def _extract_dollar_context(sentences: list[str]) -> list[str]:
    """Return sentences that contain both a dollar amount and facility/capex context."""
    out = []
    for s in sentences:
        if _DOLLAR_RE.search(s) and (_FACILITY_RE.search(s) or _PPE_HEADER_RE.search(s)):
            out.append(s)
    return out[:6]


def _summarize(quarter, ppe_sents, facility_sents, cip_items, dollar_amounts) -> str:
    if dollar_amounts:
        return dollar_amounts[0][:250]
    if cip_items:
        return cip_items[0][:250]
    if facility_sents:
        return facility_sents[0][:250]
    if ppe_sents:
        return ppe_sents[0][:250]
    return "No PP&E or facility detail found in filing."


def _empty(quarter: str, period_end: datetime) -> dict:
    return {
        "quarter":              quarter,
        "period_end":           period_end,
        "filing_date":          None,
        "accession":            None,
        "form_type":            None,
        "ppe_sentences":        [],
        "cip_items":            [],
        "commitment_sentences": [],
        "facility_descriptions": [],
        "dollar_amounts":       [],
        "summary":              "No 10-Q/10-K found in window.",
    }


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"
