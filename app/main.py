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

import logging
from pathlib import Path
from typing import Annotated

import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel

from app import runner
from logger import get_logger

log = get_logger(__name__)

BASE_DIR    = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
OUTPUT_DIR  = BASE_DIR / "output"
STATIC_DIR  = Path(__file__).parent / "static"

app = FastAPI(title="doer sprint manager", docs_url=None, redoc_url=None)


# ── auth ─────────────────────────────────────────────────────────────────────

def _web_secret() -> str:
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    return cfg.get("web", {}).get("secret", "")


async def require_auth(authorization: Annotated[str | None, Header()] = None):
    secret = _web_secret()
    if not secret or authorization != f"Bearer {secret}":
        log.warning("Unauthorized request (token mismatch or missing)")
        raise HTTPException(status_code=401, detail="Unauthorized")


Auth = Annotated[None, Depends(require_auth)]


# ── pages / static ───────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index():
    return (STATIC_DIR / "index.html").read_text()


@app.get("/output/{filename}", include_in_schema=False)
async def serve_output(filename: str, _: Auth):
    path = OUTPUT_DIR / filename
    if not path.exists() or path.suffix not in (".html", ".csv"):
        raise HTTPException(status_code=404)
    return FileResponse(path)


# ── config ───────────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config(_: Auth):
    return yaml.safe_load(CONFIG_PATH.read_text())


@app.post("/api/config")
async def update_config(data: dict, _: Auth):
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    _deep_merge(cfg, data)
    CONFIG_PATH.write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True))
    log.info("Config updated via management page")
    return {"ok": True}


# ── sprint run ────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    force_pivot: bool = False


@app.post("/api/run")
async def run_sprint(body: RunRequest, _: Auth):
    started = await runner.start(body.force_pivot)
    if not started:
        raise HTTPException(status_code=409, detail="Sprint already running")
    log.info("Sprint triggered via management page (force_pivot=%s)", body.force_pivot)
    return {"started": True}


@app.get("/api/run/status")
async def run_status(_: Auth):
    return {
        "running":    runner.state["running"],
        "started_at": runner.state["started_at"],
        "exit_code":  runner.state["exit_code"],
        "verdict":    runner.state["verdict"],
        "line_count": len(runner.state["output_lines"]),
    }


@app.get("/api/run/stream")
async def run_stream(_: Auth):
    return StreamingResponse(
        runner.output_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering for SSE
        },
    )


@app.get("/api/run/output")
async def run_output(_: Auth):
    return {"lines": runner.state["output_lines"]}


# ── logs ─────────────────────────────────────────────────────────────────────

@app.get("/api/logs")
async def get_logs(n: int = 200, _: Auth = None):
    log_path = OUTPUT_DIR / "sprint.log"
    if not log_path.exists():
        return {"lines": []}
    lines = log_path.read_text(errors="replace").splitlines()
    return {"lines": lines[-n:]}


@app.get("/api/dashboards")
async def list_dashboards(_: Auth):
    files = sorted(OUTPUT_DIR.glob("dashboard_*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {"files": [f.name for f in files]}


# ── helpers ───────────────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> None:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
