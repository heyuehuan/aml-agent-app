"""FastAPI app for the Real-Time AML Agent demo."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Load .env from project root (parent of backend/)
load_dotenv(Path(__file__).parent.parent / ".env")

import db
import streaming
from aml_agent.report_html import render_html

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.getLogger("google_genai.types").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

DEMO_PASSCODE = os.getenv("DEMO_PASSCODE", "aml-demo-2026")

app = FastAPI(title="AML Agent Demo")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def verify_passcode(x_demo_passcode: str | None = Header(default=None)) -> None:
    if x_demo_passcode != DEMO_PASSCODE:
        raise HTTPException(status_code=401, detail="Invalid or missing passcode")


# ── Health ──────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}


# ── Jobs ────────────────────────────────────────────────────────────────────

class CreateJobRequest(BaseModel):
    subject: str


@app.get("/api/jobs", dependencies=[Depends(verify_passcode)])
def list_jobs():
    return db.list_jobs()


@app.post("/api/jobs", dependencies=[Depends(verify_passcode)])
def create_job(body: CreateJobRequest):
    if not body.subject.strip():
        raise HTTPException(status_code=400, detail="subject must not be empty")
    return db.create_job(body.subject.strip())


@app.get("/api/jobs/{job_id}", dependencies=[Depends(verify_passcode)])
def get_job(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def _extract_sql_results(events: list[dict]) -> list[dict]:
    """Parse markdown tables from SQL execute tool_result events into {columns, rows} dicts."""
    results = []
    for ev in events:
        if ev.get("type") != "tool_result" or ev.get("tool") != "execute":
            continue
        text = ev.get("result", "")
        lines = [l.strip() for l in text.splitlines() if l.strip().startswith("|")]
        # Drop separator lines (only dashes/pipes)
        lines = [l for l in lines if not re.match(r"^\|[\s\-|]+\|$", l)]
        if len(lines) < 2:
            continue
        cols = [c.strip() for c in lines[0].strip("|").split("|")]
        rows = []
        for line in lines[1:]:
            row = [c.strip() for c in line.strip("|").split("|")]
            if len(row) == len(cols):
                rows.append(row)
        if cols and rows:
            results.append({"columns": cols, "rows": rows})
    return results


@app.get("/api/jobs/{job_id}/report", dependencies=[Depends(verify_passcode)])
def get_job_report(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if not job.get("report_md"):
        raise HTTPException(status_code=404, detail="Report not yet available")
    sql_results = _extract_sql_results(job.get("events", []))
    html_content = render_html(
        job["report_md"],
        subject=job["subject"],
        finished_at=job.get("finished_at"),
        sql_results=sql_results or None,
    )
    return HTMLResponse(content=html_content)


@app.delete("/api/jobs/{job_id}", dependencies=[Depends(verify_passcode)])
def delete_job(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    db.delete_job(job_id)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/execute", dependencies=[Depends(verify_passcode)])
async def execute_job(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] not in ("queued", "failed"):
        raise HTTPException(
            status_code=409,
            detail=f"Job is already {job['status']}. Only queued or failed jobs can be executed.",
        )
    if job["status"] == "failed":
        db.reset_to_queued(job_id)

    async def event_stream():
        try:
            async for event in streaming.stream_investigation(job_id, job["subject"]):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    db.init_db()
    logging.getLogger(__name__).info("DB initialised. Passcode: %s", DEMO_PASSCODE)


# ── Static frontend ──────────────────────────────────────────────────────────

_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


def main():
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
