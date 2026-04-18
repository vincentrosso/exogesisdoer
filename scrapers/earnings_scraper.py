"""
Earnings call Q&A scanner — looks for evasive management answers about
capital allocation in SEC 8-K filings (Items 2.02 & 7.01).

Heuristic:
  1. Split the filing text into Q&A blocks (analyst question + mgmt answer).
  2. Flag blocks where the QUESTION mentions capital allocation keywords.
  3. Within flagged blocks, score the ANSWER for evasion language.
  4. Return all flagged Q&A pairs with an evasion score.
"""

import re
from datetime import datetime, timedelta

from logger import get_logger
from scrapers.edgar import get_cik, get_filing_text, list_filings

log = get_logger(__name__)

CAPEX_QUESTION_RE = re.compile(
    r'\b(capex|capital expenditure|capital allocation|capital spending|'
    r'ppe|property.{0,10}equipment|facility|facilities|manufacturing|'
    r'build.out|build out|investment)\b',
    re.IGNORECASE,
)

EVASION_RE = re.compile(
    r"\b(don'?t provide|not going to|not in a position|can'?t comment|"
    r"we'?ll discuss|at this time|at a later|premature|nothing to announce|"
    r"nothing to share|not something we|stay tuned|more to come|"
    r"not prepared to|we'?re not|won'?t be|decline to|not ready|"
    r"we haven'?t finalized|under review|being evaluated)\b",
    re.IGNORECASE,
)

# Patterns that indicate a Q&A block boundary
_QA_SPLITS = [
    re.compile(r'\n\s*(?:Q\s*[-–:]|QUESTION\s*[-–:]|\[Analyst\]|\bAnalyst\b\s*:)', re.IGNORECASE),
    re.compile(r'\n\s*(?:A\s*[-–:]|ANSWER\s*[-–:]|\[(?:CEO|CFO|Management)\])', re.IGNORECASE),
]

_QUESTION_MARKERS = re.compile(
    r'^\s*(?:Q\s*[-–:]|QUESTION\s*[-–:]|\[[\w\s]+\]\s*(?:Analyst|Research))',
    re.IGNORECASE | re.MULTILINE,
)
_ANSWER_MARKERS = re.compile(
    r'^\s*(?:A\s*[-–:]|ANSWER\s*[-–:]|\[(?:CEO|CFO|President|Chief|VP|Senior)\])',
    re.IGNORECASE | re.MULTILINE,
)


def get_evasive_qa(
    ticker: str,
    anomaly_period_ends: list[datetime],
    window_days: int = 90,
) -> list[dict]:
    """
    For each anomaly quarter, scan earnings-related 8-K filings for
    Q&A blocks where management evades capital allocation questions.

    Returns list of findings:
      {quarter, filing_date, accession, question, answer, evasion_score, url}
    """
    if not anomaly_period_ends:
        return []

    log.info("[%s] Scanning earnings call Q&A for %d anomaly quarter(s)", ticker, len(anomaly_period_ends))
    cik      = get_cik(ticker)
    filings  = list_filings(cik, "8-K")
    relevant = [f for f in filings if "2.02" in f["items"] or "7.01" in f["items"]]

    findings = []

    for period_end in anomaly_period_ends:
        window_end = period_end + timedelta(days=window_days)
        quarter    = _ql(period_end)

        in_window = [
            f for f in relevant
            if period_end <= datetime.strptime(f["filingDate"], "%Y-%m-%d") <= window_end
        ]
        log.debug("[%s] %s: %d earnings 8-K(s) to scan for Q&A", ticker, quarter, len(in_window))

        for filing in in_window:
            accn      = filing["accessionNumber"]
            filed_on  = filing["filingDate"]
            try:
                text   = get_filing_text(cik, accn, filing["primaryDocument"])
                pairs  = _extract_qa_pairs(text)
                log.debug("[%s] %s filing %s: %d Q&A pair(s) extracted", ticker, quarter, accn, len(pairs))

                for q_text, a_text in pairs:
                    if not CAPEX_QUESTION_RE.search(q_text):
                        continue
                    evasion_score = len(EVASION_RE.findall(a_text))
                    log.info(
                        "[%s] %s CAPEX Q&A found (evasion_score=%d) in %s",
                        ticker, quarter, evasion_score, accn,
                    )
                    if evasion_score > 0:
                        log.info("  Q: %.100s…", q_text.strip())
                        log.info("  A: %.100s…", a_text.strip())
                    findings.append({
                        "quarter":       quarter,
                        "period_end":    period_end,
                        "filing_date":   filed_on,
                        "accession":     accn,
                        "question":      q_text.strip(),
                        "answer":        a_text.strip(),
                        "evasion_score": evasion_score,
                        "url": (
                            f"https://www.sec.gov/Archives/edgar/data/"
                            f"{int(cik)}/{accn.replace('-','')}/{filing['primaryDocument']}"
                        ),
                    })
            except Exception:
                log.warning("[%s] %s failed to scan %s", ticker, quarter, accn, exc_info=True)

    evasive = [f for f in findings if f["evasion_score"] > 0]
    log.info("[%s] Q&A scan complete: %d capital-allocation Q&A found, %d evasive",
             ticker, len(findings), len(evasive))
    return findings


def _extract_qa_pairs(text: str) -> list[tuple[str, str]]:
    """
    Split text into (question, answer) string pairs.
    Handles common earnings call transcript formats.
    """
    # Strategy 1: explicit Q: / A: markers
    q_positions = [(m.start(), m.end()) for m in _QUESTION_MARKERS.finditer(text)]
    a_positions = [(m.start(), m.end()) for m in _ANSWER_MARKERS.finditer(text)]

    pairs = []
    for qs, qe in q_positions:
        # Find the first A: that comes after this Q:
        answer_start = next((ae for as_, ae in a_positions if as_ > qs), None)
        if answer_start is None:
            continue
        # Find the next Q: or A: to bound the answer
        next_boundary = min(
            (s for s, _ in q_positions + a_positions if s > answer_start),
            default=len(text),
        )
        q_text = text[qe:answer_start].strip()
        a_text = text[answer_start:next_boundary].strip()
        if len(q_text) > 20 and len(a_text) > 20:
            pairs.append((q_text, a_text))

    # Strategy 2: paragraph-level heuristic if no structured markers found
    if not pairs:
        paragraphs = [p.strip() for p in text.split('\n\n') if len(p.strip()) > 40]
        for i, para in enumerate(paragraphs[:-1]):
            if CAPEX_QUESTION_RE.search(para) and '?' in para:
                answer = paragraphs[i + 1]
                pairs.append((para, answer))

    return pairs


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"
