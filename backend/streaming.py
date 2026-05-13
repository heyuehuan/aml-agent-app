"""ADK agent runner → SSE event stream for the AML demo.

Runs the ADK agent, translates each event part into a typed JSON dict,
and yields them for consumption by the FastAPI SSE endpoint.
Saves the full event log + report to the job store when complete.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
import re
import uuid
from collections.abc import AsyncGenerator
from typing import Any

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

import db
from aml_agent.agent import create_aml_agent
from aml_agent.config import Configs

logger = logging.getLogger(__name__)

# Suppress noisy upstream warnings
import warnings
warnings.filterwarnings("ignore", message="Inheritance class AiohttpClientSession", category=DeprecationWarning)
logging.getLogger("google_genai.types").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)


def _extract_risk_level(markdown: str) -> str | None:
    m = re.search(r"Risk Assessment:\s*(HIGH|MEDIUM|LOW|CLEAR)", markdown, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


async def stream_investigation(
    job_id: str,
    subject: str,
    configs: Configs | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run an AML investigation and yield SSE events as dicts.

    Events are yielded as they arrive; the job DB record is updated at the end.
    """
    if configs is None:
        configs = Configs.from_env()

    db.mark_running(job_id)
    yield {"type": "status", "message": f"Starting AML investigation for: {subject}"}

    accumulated_events: list[dict[str, Any]] = []
    final_report_md = ""
    risk_level: str | None = None

    def _emit(event: dict[str, Any]) -> dict[str, Any]:
        accumulated_events.append(event)
        if len(accumulated_events) % 5 == 0:
            db.update_events_partial(job_id, accumulated_events)
        return event

    try:
        agent = create_aml_agent(configs=configs)

        runner = Runner(
            app_name="aml_demo",
            agent=agent,
            session_service=InMemorySessionService(),
        )

        session_id = str(uuid.uuid4())
        await runner.session_service.create_session(
            app_name="aml_demo",
            user_id="demo_user",
            session_id=session_id,
        )

        message = types.Content(
            parts=[types.Part(text=f"Investigate the following subject for AML compliance:\n\n{subject}")],
            role="user",
        )

        # Track pending tool calls so we can pair them with responses (FIFO per tool name)
        pending_tools: dict[str, list[str]] = {}  # tool_name → [call_id, ...]

        async for event in runner.run_async(
            session_id=session_id,
            user_id="demo_user",
            new_message=message,
        ):
            if not event.content or not event.content.parts:
                continue

            for part in event.content.parts:
                is_thought = getattr(part, "thought", False)

                # Thinking / reasoning text
                if is_thought and part.text:
                    evt = _emit({"type": "thinking", "content": part.text})
                    yield evt
                    continue

                # Tool call
                fc = getattr(part, "function_call", None)
                if fc:
                    call_id = getattr(fc, "id", None) or fc.name
                    args = dict(fc.args) if fc.args else {}
                    pending_tools.setdefault(fc.name, []).append(call_id)
                    evt = _emit({"type": "tool_call", "tool": fc.name, "args": args, "call_id": call_id})
                    yield evt
                    continue

                # Tool response
                fr = getattr(part, "function_response", None)
                if fr:
                    resp = fr.response or {}
                    if isinstance(resp, dict):
                        result_text = resp.get("result", str(resp))
                    else:
                        result_text = str(resp)
                    evt = _emit({
                        "type": "tool_result",
                        "tool": fr.name,
                        "result": result_text,
                    })
                    yield evt
                    continue

                # Regular text output (non-thought)
                if part.text:
                    evt = _emit({"type": "text", "content": part.text})
                    yield evt

            # Final response → extract report
            if event.is_final_response() and event.content:
                final_report_md = "".join(
                    p.text or ""
                    for p in event.content.parts
                    if p.text and not getattr(p, "thought", False)
                )
                # Strip preamble before the formal heading
                match = re.search(r"(#\s+AML Investigation Report:)", final_report_md)
                if match:
                    final_report_md = final_report_md[match.start():]

                risk_level = _extract_risk_level(final_report_md)
                evt = _emit({
                    "type": "report",
                    "markdown": final_report_md,
                    "risk_level": risk_level or "UNKNOWN",
                    "subject": subject,
                })
                yield evt

        # Clean up agent tools
        await runner.close()
        for tool in getattr(agent, "tools", []):
            inner = getattr(tool, "func", None)
            owner = getattr(inner, "__self__", None) if inner else None
            if owner and hasattr(owner, "close"):
                try:
                    owner.close()
                except Exception:
                    pass

        db.mark_complete(job_id, accumulated_events, final_report_md, risk_level)
        yield {"type": "done"}

    except Exception as exc:
        logger.exception("stream_investigation failed for job %s", job_id)
        err_evt = {"type": "error", "message": str(exc)}
        accumulated_events.append(err_evt)
        yield err_evt
        db.mark_failed(job_id, str(exc), accumulated_events)
        yield {"type": "done"}
