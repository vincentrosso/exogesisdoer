"""
Sprint subprocess manager.

Runs run.py in a background asyncio task and exposes its output
via an asyncio.Queue for SSE streaming. Module-level state is
intentionally simple — we only ever run one sprint at a time.
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

from logger import get_logger

log = get_logger(__name__)

BASE_DIR = Path(__file__).parent.parent

state: dict = {
    "running":    False,
    "started_at": None,   # ISO-8601 UTC string
    "exit_code":  None,   # int when finished, None while running/never run
    "verdict":    None,   # "ANOMALY" | "PIVOT" | "NULL" | None
    "output_lines": [],
}

_queue: asyncio.Queue | None = None


def _q() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue


async def start(force_pivot: bool = False) -> bool:
    """Start a sprint run. Returns False if one is already in progress."""
    if state["running"]:
        log.warning("start() called while sprint already running — ignored")
        return False

    state["running"]    = True
    state["started_at"] = datetime.now(timezone.utc).isoformat()
    state["exit_code"]  = None
    state["verdict"]    = None
    state["output_lines"] = []

    q = _q()
    while not q.empty():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            break

    log.info("Sprint subprocess starting (force_pivot=%s)", force_pivot)
    asyncio.create_task(_run(force_pivot))
    return True


async def _run(force_pivot: bool) -> None:
    python = str(BASE_DIR / ".venv" / "bin" / "python")
    if not Path(python).exists():
        python = sys.executable

    cmd = [python, str(BASE_DIR / "run.py")]
    if force_pivot:
        cmd.append("--force-pivot")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(BASE_DIR),
        )
        q = _q()
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            state["output_lines"].append(line)
            await q.put(line)
            _parse_verdict(line)

        await proc.wait()
        state["exit_code"] = proc.returncode
        log.info("Sprint subprocess finished (exit_code=%s, verdict=%s)", proc.returncode, state["verdict"])

    except Exception:
        log.error("Sprint subprocess crashed", exc_info=True)
        state["exit_code"] = -1
    finally:
        state["running"] = False
        await _q().put(None)  # sentinel — closes SSE stream


def _parse_verdict(line: str) -> None:
    if "VERDICT: ANOMALY FOUND" in line:
        state["verdict"] = "ANOMALY"
    elif "VERDICT: No anomaly found in either" in line:
        state["verdict"] = "NULL"
    elif "ACTION:  PIVOTING" in line and state["verdict"] is None:
        state["verdict"] = "PIVOT_RUNNING"


async def output_stream():
    """
    AsyncGenerator yielding SSE-formatted lines.
    Immediately replays buffered output if a run already completed,
    then streams live lines while running.
    """
    q = _q()

    # Replay any buffered lines first (e.g., client connected mid-run)
    for line in list(state["output_lines"]):
        yield f"data: {_sse_json(line)}\n\n"

    if not state["running"]:
        yield "data: [DONE]\n\n"
        return

    while True:
        try:
            item = await asyncio.wait_for(q.get(), timeout=25.0)
        except asyncio.TimeoutError:
            yield ": keepalive\n\n"
            continue
        if item is None:
            yield "data: [DONE]\n\n"
            break
        yield f"data: {_sse_json(item)}\n\n"


def _sse_json(line: str) -> str:
    import json
    return json.dumps(line)
