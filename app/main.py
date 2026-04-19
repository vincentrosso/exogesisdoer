"""
FastAPI management web service for the sprint tool.

Routes:
  GET  /                       — management page HTML
  GET  /output/{file}          — serve generated dashboard HTML files
  GET  /api/config             — read config.yaml
  POST /api/config             — write config.yaml (partial merge)
  POST /api/run                — start sprint run
  GET  /api/run/status         — current run state
  GET  /api/run/stream         — SSE live output stream
  GET  /api/run/output         — full output of last/current run
  GET  /api/logs               — last N lines of sprint.log
  GET  /api/dashboards         — list generated HTML dashboard files
"""

from pathlib import Path

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel

from app import runner
from logger import get_logger

log = get_logger(__name__)

BASE_DIR       = Path(__file__).parent.parent
CONFIG_PATH    = BASE_DIR / "config.yaml"
UNIVERSE_PATH  = BASE_DIR / "biotech_universe.yaml"
OUTPUT_DIR     = BASE_DIR / "output"
STATIC_DIR     = Path(__file__).parent / "static"

app = FastAPI(title="doer sprint manager", docs_url=None, redoc_url=None)


# ── pages / static ───────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index():
    return (STATIC_DIR / "index.html").read_text()


@app.get("/output/{filename}", include_in_schema=False)
async def serve_output(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists() or path.suffix not in (".html", ".csv"):
        raise HTTPException(status_code=404)
    return FileResponse(path)


# ── config ───────────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config():
    return yaml.safe_load(CONFIG_PATH.read_text())


@app.post("/api/config")
async def update_config(data: dict):
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    _deep_merge(cfg, data)
    CONFIG_PATH.write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True))
    log.info("Config updated via management page")
    return {"ok": True}


# ── sprint run ────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    force_pivot: bool = False


@app.post("/api/run")
async def run_sprint(body: RunRequest):
    started = await runner.start(body.force_pivot)
    if not started:
        raise HTTPException(status_code=409, detail="Sprint already running")
    log.info("Sprint triggered via management page (force_pivot=%s)", body.force_pivot)
    return {"started": True}


class SweepRequest(BaseModel):
    limit: int = 0
    ticker: str = ""


@app.post("/api/sweep")
async def run_sweep(body: SweepRequest):
    if runner.state["running"]:
        raise HTTPException(status_code=409, detail="A run is already in progress")

    args = []
    if body.limit:
        args += ["--limit", str(body.limit)]
    if body.ticker:
        args += ["--ticker", body.ticker]

    import sys
    python = str(BASE_DIR / ".venv" / "bin" / "python")
    if not Path(python).exists():
        python = sys.executable

    cmd = [python, str(BASE_DIR / "sweep.py")] + args
    runner.state["running"]      = True
    runner.state["started_at"]   = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    runner.state["exit_code"]    = None
    runner.state["verdict"]      = None
    runner.state["output_lines"] = []

    import asyncio
    asyncio.create_task(_run_sweep_process(cmd))
    log.info("Sweep triggered via management page (args=%s)", args)
    return {"started": True}


async def _run_sweep_process(cmd: list):
    q = runner._q()
    while not q.empty():
        try: q.get_nowait()
        except Exception: break
    try:
        proc = await __import__("asyncio").create_subprocess_exec(
            *cmd,
            stdout=__import__("asyncio").subprocess.PIPE,
            stderr=__import__("asyncio").subprocess.STDOUT,
            cwd=str(BASE_DIR),
        )
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            runner.state["output_lines"].append(line)
            await q.put(line)
        await proc.wait()
        runner.state["exit_code"] = proc.returncode
        runner.state["verdict"] = "SWEEP_DONE"
    except Exception:
        log.error("Sweep process crashed", exc_info=True)
        runner.state["exit_code"] = -1
    finally:
        runner.state["running"] = False
        await q.put(None)


@app.get("/api/run/status")
async def run_status():
    return {
        "running":    runner.state["running"],
        "started_at": runner.state["started_at"],
        "exit_code":  runner.state["exit_code"],
        "verdict":    runner.state["verdict"],
        "line_count": len(runner.state["output_lines"]),
    }


@app.get("/api/run/stream")
async def run_stream():
    return StreamingResponse(
        runner.output_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/run/output")
async def run_output():
    return {"lines": runner.state["output_lines"]}


# ── logs ─────────────────────────────────────────────────────────────────────

@app.get("/api/logs")
async def get_logs(n: int = 200):
    log_path = OUTPUT_DIR / "sprint.log"
    if not log_path.exists():
        return {"lines": []}
    lines = log_path.read_text(errors="replace").splitlines()
    return {"lines": lines[-n:]}


class DeepResearchRequest(BaseModel):
    ticker: str
    company_name: str
    anomaly_quarters: list[str]        # e.g. ["2025-Q4", "2025-Q2"]
    anomaly_period_ends: list[str]     # ISO dates e.g. ["2025-12-31"]
    cik_override: str | None = None


@app.post("/api/deep-research")
async def deep_research(body: DeepResearchRequest):
    import asyncio
    from datetime import datetime as _dt

    period_ends = [_dt.strptime(d, "%Y-%m-%d") for d in body.anomaly_period_ends]

    def _run():
        from scrapers.form4_scraper  import get_insider_transactions
        from scrapers.tenq_scraper   import get_ppe_analysis
        from report.deep_research    import generate_deep_report

        insider_txns = get_insider_transactions(
            body.ticker, period_ends, cik_override=body.cik_override)
        ppe_analysis = get_ppe_analysis(
            body.ticker, period_ends, cik_override=body.cik_override)

        ts          = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M")
        report_path = OUTPUT_DIR / f"deep_{body.ticker}_{ts}.html"
        _, findings = generate_deep_report(
            ticker           = body.ticker,
            company_name     = body.company_name,
            anomaly_quarters = body.anomaly_quarters,
            insider_txns     = insider_txns,
            ppe_analysis     = ppe_analysis,
            output_path      = report_path,
        )
        return report_path.name, findings

    loop               = asyncio.get_event_loop()
    filename, findings = await loop.run_in_executor(None, _run)
    log.info("Deep research report generated: %s", filename)
    return {"report_url": f"/output/{filename}", "findings": findings}


@app.get("/api/sweep/results")
async def sweep_results():
    import json as _json
    path = OUTPUT_DIR / "sweep_latest.json"
    if not path.exists():
        return {"generated_at": None, "scanned": 0, "hits": 0, "ranked": [], "report_file": None}
    return _json.loads(path.read_text())


@app.get("/api/universe")
async def get_universe():
    data = yaml.safe_load(UNIVERSE_PATH.read_text())
    return {"companies": data.get("companies", [])}


class BatchSummaryRequest(BaseModel):
    results: list[dict]   # [{ticker, name, quarter, qoq_pct, report_url, deep_report_url, findings}]


@app.post("/api/batch-summary")
async def batch_summary(body: BatchSummaryRequest):
    import asyncio as _asyncio
    from datetime import datetime as _dt

    def _run():
        from report.batch_summary import generate_batch_summary
        ts   = _dt.now().strftime("%Y%m%d_%H%M")
        path = OUTPUT_DIR / f"batch_summary_{ts}.html"
        generate_batch_summary(body.results, path)
        return path.name

    loop     = _asyncio.get_event_loop()
    filename = await loop.run_in_executor(None, _run)
    log.info("Batch summary generated: %s", filename)
    return {"report_url": f"/output/{filename}"}


@app.get("/api/dashboards")
async def list_dashboards():
    files = sorted(
        list(OUTPUT_DIR.glob("dashboard_*.html")) +
        list(OUTPUT_DIR.glob("report_*.html")) +
        list(OUTPUT_DIR.glob("batch_summary_*.html")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return {"files": [f.name for f in files]}


# ── helpers ───────────────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> None:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
