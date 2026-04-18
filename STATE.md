# Sprint State Document
**Last Updated:** 2026-04-18  
**Status:** AUTHORIZED — Ready to Execute

---

## Objective

Generate a "spectacle" report for quantitative hedge funds by proving a specific, machine-testable anomaly at a mid-cap biotech firm using only public data. Constraint: 48 hours.

---

## The One True Thing

> Under the 48-hour constraint, the only viable signal is a simplified, binary test comparing a standardized measure of past capital expenditure against the presence or absence of quantified forward-looking justification.

---

## Signal Definition

### Condition A — CapEx Spike (Past Action)

| Field | Value |
|---|---|
| **Source** | SEC 10-Q filings → Statement of Cash Flows |
| **Line Item** | `Payments for purchase of property and equipment` |
| **Calculation** | Quarter-over-quarter % change (current vs. prior quarter) |
| **Threshold** | ≥ 40% increase → **SPIKE = 1** |

### Condition B — Quantified Forward Guidance Flag (Future Communication)

| Field | Value |
|---|---|
| **Source** | SEC Form 8-K filings (Items 2.02 & 7.01) |
| **Method** | Regex scan for co-occurrence within a single sentence |
| **Flag = 1 if** | Sentence contains ALL THREE: currency figure (`$XM`) **AND** expansion noun **AND** future-tense language |
| **Flag = 0 if** | No such sentence found |

**Expansion nouns:** `capacity`, `facility`, `manufacturing`, `software`, `platform`, `digital`

**Future-tense language:** `will`, `plan to`, `expect to`, `guidance for`, `coming online`

---

## Anomaly Definition

A quarter satisfies the signal **if and only if**:

```
Condition A: CapEx Spike ≥ 40%
    AND
Condition B: QFG Flag = 0
```

Both must be true. Either alone is not an anomaly.

---

## Operational Sequence

### Hours 0–12: Scraper Build & Run

- Build two independent scrapers (10-Q cash flow parser, 8-K regex scanner)
- Target: primary biotech firm + 2 pre-selected peers
- Lookback: last 6–8 quarters
- **Output:** Dashboard with:
  - Bar chart: CapEx % change by quarter
  - Point overlay: QFG flag (0/1) by quarter

### Hours 12–48: Secondary Work (Conditional)

**Trigger:** At least one quarter found where both conditions are met for the primary target.

If triggered:
1. Confirm flat clinical trial enrollment on ClinicalTrials.gov
2. Targeted scan of earnings call Q&A for evasive answers on capital allocation
3. Build narrative → produce spectacle report

**If not triggered → Pivot immediately** (see below)

---

## Pivot Condition

If no anomaly quarter is found for the biotech target:

- Immediately pivot to the predefined **software firm target**
- Apply identical scrapers
- CapEx line item may differ: `Capitalized software development costs`
- No time spent on biotech secondary work

---

## Accepted Compromises

| Compromise | Accepted Risk |
|---|---|
| Backward-looking cash spend (not forward commitments) | Sacrifices signal lead time for data reliability and scraper simplicity |
| Broad 8-K regex filter | A vague, low-dollar justification can produce Flag = 1, masking a true anomaly (false negative risk). Accepted to avoid missing large-scale legitimate justifications. |

---

## Output States

| Result | Action |
|---|---|
| Anomaly found (primary target) | Proceed to secondary work → produce spectacle report |
| No anomaly (primary target) | Pivot to software target → run scrapers → repeat decision |
| No anomaly (both targets) | Sprint concludes with null result |
