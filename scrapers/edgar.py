"""
EDGAR API client with file-based caching and polite rate limiting.
All public functions return plain dicts/strings; callers handle parsing.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from logger import get_logger

log = get_logger(__name__)

HEADERS = {"User-Agent": "exogesisdoer sprint vincentrosso@gmail.com"}
DATA_BASE = "https://data.sec.gov"
WWW_BASE  = "https://www.sec.gov"
CACHE_DIR = Path(__file__).parent.parent / "cache"
CACHE_TTL  = timedelta(hours=24)
MIN_INTERVAL = 0.12  # stay safely under EDGAR's 10 req/s limit

_last_req_time: float = 0.0


def _get(url: str) -> requests.Response:
    global _last_req_time
    wait = MIN_INTERVAL - (time.time() - _last_req_time)
    if wait > 0:
        time.sleep(wait)
    log.debug("GET %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        log.error("HTTP error fetching %s — status %s", url, exc.response.status_code, exc_info=True)
        raise
    except requests.exceptions.ConnectionError:
        log.error("Connection error fetching %s", url, exc_info=True)
        raise
    except requests.exceptions.Timeout:
        log.error("Timeout fetching %s", url, exc_info=True)
        raise
    _last_req_time = time.time()
    log.debug("  → %s bytes", len(r.content))
    return r


def _cached_json(key: str, fetch_url: str) -> dict:
    CACHE_DIR.mkdir(exist_ok=True)
    path = CACHE_DIR / f"{key}.json"
    if path.exists():
        age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
        if age < CACHE_TTL:
            log.debug("Cache hit: %s (age %s)", key, str(age).split(".")[0])
            return json.loads(path.read_text())
        log.debug("Cache stale: %s (age %s)", key, str(age).split(".")[0])
    else:
        log.debug("Cache miss: %s", key)

    data = _get(fetch_url).json()
    path.write_text(json.dumps(data))
    log.debug("Cache written: %s", path.name)
    return data


def get_cik(ticker: str) -> str:
    """Return zero-padded 10-digit CIK string for a ticker symbol."""
    log.debug("Resolving CIK for ticker: %s", ticker)
    data = _cached_json("company_tickers", f"{WWW_BASE}/files/company_tickers.json")
    upper = ticker.upper()
    for entry in data.values():
        if entry["ticker"] == upper:
            cik = str(entry["cik_str"]).zfill(10)
            log.debug("  CIK for %s → %s", ticker, cik)
            return cik
    log.error("Ticker '%s' not found in EDGAR company tickers", ticker)
    raise ValueError(f"Ticker '{ticker}' not found in EDGAR company tickers")


def get_company_facts(cik: str) -> dict:
    """Full XBRL company facts JSON (cached 24h). Accepts raw or zero-padded CIK."""
    cik = str(int(cik)).zfill(10)  # normalise to 10-digit zero-padded
    log.debug("Fetching company facts for CIK %s", cik)
    return _cached_json(
        f"{cik}_facts",
        f"{DATA_BASE}/api/xbrl/companyfacts/CIK{cik}.json",
    )


def get_submissions(cik: str) -> dict:
    """EDGAR submissions JSON (recent filings + metadata)."""
    log.debug("Fetching submissions for CIK %s", cik)
    return _cached_json(
        f"{cik}_submissions",
        f"{DATA_BASE}/submissions/CIK{cik}.json",
    )


def get_filing_text(cik: str, accession_no: str, primary_doc: str) -> str:
    """
    Download primary document of a filing and return plain text.
    HTML is stripped via BeautifulSoup; .txt files returned as-is.
    """
    cik_int = int(cik)
    acc_nodash = accession_no.replace("-", "")
    url = f"{WWW_BASE}/Archives/edgar/data/{cik_int}/{acc_nodash}/{primary_doc}"
    log.debug("Downloading filing text: %s / %s", accession_no, primary_doc)
    r = _get(url)
    if primary_doc.lower().endswith((".htm", ".html")):
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(separator=" ", strip=True)
        log.debug("  Parsed HTML → %d chars", len(text))
        return text
    return r.text


def list_filings(cik: str, form_type: str) -> list[dict]:
    """
    Return list of filings of a given form type from EDGAR submissions.
    Each dict has: accessionNumber, filingDate, primaryDocument, items.
    Handles pagination via the 'files' key for older filings.
    """
    log.debug("Listing %s filings for CIK %s", form_type, cik)
    sub = get_submissions(cik)
    recent = sub.get("filings", {}).get("recent", {})

    forms   = recent.get("form", [])
    dates   = recent.get("filingDate", [])
    accns   = recent.get("accessionNumber", [])
    docs    = recent.get("primaryDocument", [])
    items   = recent.get("items", [])

    results = []
    for i, f in enumerate(forms):
        if f == form_type:
            results.append({
                "accessionNumber": accns[i],
                "filingDate":      dates[i],
                "primaryDocument": docs[i],
                "items":           items[i] if i < len(items) else "",
            })

    # Older filings may be in additional index files — fetch if needed
    for extra in sub.get("filings", {}).get("files", []):
        if extra.get("name", "").endswith(".json"):
            try:
                extra_data = _cached_json(
                    f"{cik}_sub_{extra['name']}",
                    f"{DATA_BASE}/submissions/{extra['name']}",
                )
                e_forms = extra_data.get("form", [])
                e_dates = extra_data.get("filingDate", [])
                e_accns = extra_data.get("accessionNumber", [])
                e_docs  = extra_data.get("primaryDocument", [])
                e_items = extra_data.get("items", [])
                for i, f in enumerate(e_forms):
                    if f == form_type:
                        results.append({
                            "accessionNumber": e_accns[i],
                            "filingDate":      e_dates[i],
                            "primaryDocument": e_docs[i],
                            "items":           e_items[i] if i < len(e_items) else "",
                        })
            except Exception:
                log.warning(
                    "Failed to fetch supplemental submissions file '%s' for CIK %s",
                    extra.get("name"), cik, exc_info=True,
                )

    log.debug("  Found %d %s filings for CIK %s", len(results), form_type, cik)
    return sorted(results, key=lambda x: x["filingDate"])
