"""
ClinicalTrials.gov scraper — checks enrollment status for a sponsor's trials.

Uses the ClinicalTrials.gov API v2 (public, no auth).
"Flat enrollment" signal: trials that are Recruiting but have Actual
(not Estimated) enrollment counts, or are Suspended/Terminated.
"""

import time
from datetime import datetime

import requests

from logger import get_logger

log = get_logger(__name__)

CT_API   = "https://clinicaltrials.gov/api/v2/studies"
HEADERS  = {"User-Agent": "exogesisdoer sprint vincentrosso@gmail.com"}
PAGE_SIZE = 20

FLAT_STATUSES = {
    "SUSPENDED", "TERMINATED", "WITHDRAWN",
    "ACTIVE_NOT_RECRUITING", "NOT_YET_RECRUITING",
}


def get_trials(company_name: str, max_pages: int = 3) -> list[dict]:
    """
    Fetch all trials for a sponsor and return parsed trial records.
    Each record: nct_id, title, status, enrollment_count, enrollment_type,
                 phase, start_date, completion_date, flat_signal
    """
    log.info("ClinicalTrials.gov: querying sponsor '%s'", company_name)
    trials = []
    token  = None

    for page in range(max_pages):
        params = {
            "query.spons": company_name,
            "format":      "json",
            "pageSize":    PAGE_SIZE,
            "fields":      (
                "NCTId,BriefTitle,OverallStatus,EnrollmentCount,"
                "EnrollmentType,StartDate,CompletionDate,Phase"
            ),
        }
        if token:
            params["pageToken"] = token

        try:
            time.sleep(0.3)
            r = requests.get(CT_API, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception:
            log.error("ClinicalTrials.gov request failed (page %d)", page + 1, exc_info=True)
            break

        for study in data.get("studies", []):
            record = _parse_study(study)
            if record:
                trials.append(record)
            log.debug("  trial %s — status=%s enrollment=%s(%s)",
                      record.get("nct_id") if record else "?",
                      record.get("status") if record else "?",
                      record.get("enrollment_count") if record else "?",
                      record.get("enrollment_type") if record else "?")

        token = data.get("nextPageToken")
        if not token:
            break

    flat   = [t for t in trials if t["flat_signal"]]
    active = [t for t in trials if not t["flat_signal"]]
    log.info(
        "ClinicalTrials.gov: %d total trials — %d flat/concerning, %d active",
        len(trials), len(flat), len(active),
    )
    return trials


def _parse_study(study: dict) -> dict | None:
    try:
        ps  = study.get("protocolSection", {})
        idf = ps.get("identificationModule", {})
        sts = ps.get("statusModule", {})
        des = ps.get("designModule", {})

        status    = sts.get("overallStatus", "UNKNOWN").upper().replace(" ", "_")
        enroll    = des.get("enrollmentInfo", {})
        count     = enroll.get("count")
        enroll_type = enroll.get("type", "").upper()  # "ACTUAL" or "ESTIMATED"

        flat_signal = (
            status in FLAT_STATUSES
            or (status == "RECRUITING" and enroll_type == "ACTUAL" and (count or 0) < 10)
        )

        return {
            "nct_id":           idf.get("nctId", ""),
            "title":            idf.get("briefTitle", ""),
            "status":           status,
            "enrollment_count": count,
            "enrollment_type":  enroll_type,
            "phase":            (des.get("phases") or [""])[0],
            "start_date":       (sts.get("startDateStruct") or {}).get("date", ""),
            "completion_date":  (sts.get("completionDateStruct") or {}).get("date", ""),
            "flat_signal":      flat_signal,
            "url":              f"https://clinicaltrials.gov/study/{idf.get('nctId', '')}",
        }
    except Exception:
        log.warning("Failed to parse study record", exc_info=True)
        return None
