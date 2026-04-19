"""
Form 4 insider transaction scraper.

For a given ticker and anomaly period, fetches Form 4 filings from EDGAR
and extracts buy/sell transactions. Clusters of insider buying near a
capex spike are a bullish confirmation; heavy selling is bearish context.
"""

import re
from datetime import datetime, timedelta

from logger import get_logger
from scrapers.edgar import get_cik, get_filing_text, list_filings

log = get_logger(__name__)

_SHARES_RE  = re.compile(r'[\d,]+\.?\d*')
_PRICE_RE   = re.compile(r'\$\s*[\d,]+\.?\d*')


def get_insider_transactions(
    ticker: str,
    anomaly_period_ends: list[datetime],
    window_days: int = 180,
    cik_override: str | None = None,
) -> list[dict]:
    """
    Return insider transactions filed within window_days of each anomaly
    quarter end. Looks at Form 4 (and Form 4/A amendments).

    Returns list of dicts:
      {quarter, filing_date, filer_name, filer_title,
       transaction_date, transaction_type, shares, price_per_share,
       total_value, is_buy, accession}
    """
    if not anomaly_period_ends:
        return []

    log.info("[%s] Fetching Form 4 insider transactions", ticker)
    cik     = cik_override if cik_override else get_cik(ticker)
    filings = list_filings(cik, "4") + list_filings(cik, "4/A")
    filings.sort(key=lambda x: x["filingDate"])

    results = []
    for period_end in anomaly_period_ends:
        quarter     = _ql(period_end)
        window_start = period_end - timedelta(days=30)   # 30 days before quarter end
        window_end   = period_end + timedelta(days=window_days)

        in_window = [
            f for f in filings
            if window_start <= datetime.strptime(f["filingDate"], "%Y-%m-%d") <= window_end
        ]
        log.debug("[%s] %s: %d Form 4(s) in window", ticker, quarter, len(in_window))

        for filing in in_window:
            accn     = filing["accessionNumber"]
            filed_on = filing["filingDate"]
            try:
                text = get_filing_text(cik, accn, filing["primaryDocument"])
                txns = _parse_form4(text, quarter, filed_on, accn)
                log.debug("[%s] %s: filing %s → %d transaction(s)", ticker, quarter, accn, len(txns))
                results.extend(txns)
            except Exception:
                log.warning("[%s] Form 4 parse failed for %s", ticker, accn, exc_info=True)

    # Deduplicate by (filer, transaction_date, shares)
    seen = set()
    deduped = []
    for r in results:
        key = (r["filer_name"], r["transaction_date"], r["shares"], r["transaction_type"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    buys  = [r for r in deduped if r["is_buy"]]
    sells = [r for r in deduped if not r["is_buy"]]
    log.info("[%s] Insider transactions: %d buy(s), %d sell(s) across %d anomaly quarter(s)",
             ticker, len(buys), len(sells), len(anomaly_period_ends))
    return deduped


def _parse_form4(text: str, quarter: str, filed_on: str, accn: str) -> list[dict]:
    """
    Extract transactions from Form 4 plain text. EDGAR Form 4s are XML-
    based; after HTML stripping we look for key fields by regex/proximity.
    """
    results = []

    # Filer name — appears near "reportingOwner" or after "Name of Reporting Person"
    filer_name  = _extract_field(text, [
        r'(?:Name of Reporting Person|Reporting Owner Name)[:\s]+([A-Z][A-Za-z\s\-,\.]+?)(?:\n|  )',
        r'<rptOwnerName>([^<]+)</rptOwnerName>',
    ]) or "Unknown"

    filer_title = _extract_field(text, [
        r'(?:Relationship of Reporting Person|Officer Title)[:\s]+([A-Za-z\s\-,\/]+?)(?:\n|  )',
        r'<officerTitle>([^<]+)</officerTitle>',
        r'<isDirector>1</isDirector>',
    ]) or ""
    if "<isDirector>1</isDirector>" in text and not filer_title:
        filer_title = "Director"
    if "<isOfficer>1</isOfficer>" in text and not filer_title:
        filer_title = "Officer"

    # Transaction table rows — look for date + code + shares + price patterns
    # Form 4 XML has <nonDerivativeTransaction> blocks
    txn_blocks = re.findall(
        r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>',
        text, re.DOTALL | re.IGNORECASE,
    )

    if txn_blocks:
        for block in txn_blocks:
            txn = _parse_xml_txn_block(block, quarter, filed_on, accn, filer_name, filer_title)
            if txn:
                results.append(txn)
    else:
        # Fallback: plain-text heuristic
        txn = _parse_plaintext_fallback(text, quarter, filed_on, accn, filer_name, filer_title)
        if txn:
            results.append(txn)

    return results


def _parse_xml_txn_block(block: str, quarter, filed_on, accn, filer_name, filer_title) -> dict | None:
    date_m  = re.search(r'<transactionDate>\s*<value>(\d{4}-\d{2}-\d{2})', block)
    code_m  = re.search(r'<transactionCode>\s*([A-Z])\s*</transactionCode>', block)
    shares_m = re.search(r'<transactionShares>\s*<value>([\d,\.]+)', block)
    price_m  = re.search(r'<transactionPricePerShare>\s*<value>([\d,\.]+)', block)

    if not (date_m and code_m and shares_m):
        return None

    code    = code_m.group(1)
    is_buy  = code in ("P", "A", "M")  # P=open-market buy, A=award, M=option exercise
    is_sell = code in ("S", "F", "D")

    if not (is_buy or is_sell):
        return None

    shares = float(shares_m.group(1).replace(",", ""))
    price  = float(price_m.group(1).replace(",", "")) if price_m else 0.0
    value  = round(shares * price)

    return {
        "quarter":          quarter,
        "filing_date":      filed_on,
        "transaction_date": date_m.group(1),
        "filer_name":       filer_name.strip(),
        "filer_title":      filer_title.strip(),
        "transaction_type": "BUY" if is_buy else "SELL",
        "transaction_code": code,
        "shares":           shares,
        "price_per_share":  price,
        "total_value":      value,
        "is_buy":           is_buy,
        "accession":        accn,
    }


def _parse_plaintext_fallback(text, quarter, filed_on, accn, filer_name, filer_title) -> dict | None:
    # Simple heuristic: look for "Purchase" or "Sale" near a dollar figure
    buy_m  = re.search(r'\b(?:Purchase|Acquisition|Bought)\b.*?\$([\d,\.]+)', text, re.IGNORECASE)
    sell_m = re.search(r'\b(?:Sale|Sold|Disposed)\b.*?\$([\d,\.]+)', text, re.IGNORECASE)

    if not (buy_m or sell_m):
        return None

    is_buy = bool(buy_m)
    m      = buy_m or sell_m
    value  = float(m.group(1).replace(",", ""))

    return {
        "quarter":          quarter,
        "filing_date":      filed_on,
        "transaction_date": filed_on,
        "filer_name":       filer_name.strip(),
        "filer_title":      filer_title.strip(),
        "transaction_type": "BUY" if is_buy else "SELL",
        "transaction_code": "P" if is_buy else "S",
        "shares":           0.0,
        "price_per_share":  0.0,
        "total_value":      value,
        "is_buy":           is_buy,
        "accession":        accn,
    }


def _extract_field(text: str, patterns: list[str]) -> str | None:
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m and m.lastindex and m.lastindex >= 1:
            return m.group(1).strip()
    return None


def _ql(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"
