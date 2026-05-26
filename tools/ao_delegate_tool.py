"""Hermes tool for delegating durable coding work to Agent Orchestrator."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from tools.ao_bridge import AOBridge, AOBridgeError, AOSession
from tools.registry import registry, tool_error, tool_result


AO_DELEGATE_TASK_SCHEMA = {
    "name": "ao_delegate_task",
    "description": (
        "Spawn an Agent Orchestrator worker in an isolated worktree and stream "
        "its live status as subagent events. Use this for durable coding work "
        "that should run under AO, not for short reasoning-only subtasks."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "goal": {
                "type": "string",
                "description": "Short title for the AO worker row.",
            },
            "prompt": {
                "type": "string",
                "description": "Self-contained instructions for the AO worker.",
            },
            "project_id": {
                "type": "string",
                "description": (
                    "AO project id from agent-orchestrator.yaml. "
                    "Use OrynWorkspace for Oryn app work, OrynPlatform for platform/Hermes work, or Oryn for Oryn.ai app work."
                ),
                "default": "OrynWorkspace",
            },
            "issue_id": {
                "type": "string",
                "description": "Optional Linear/GitHub issue id to link the AO session.",
            },
            "branch": {
                "type": "string",
                "description": (
                    "Optional existing local or remote branch for the AO worktree. "
                    "Use this for local validation branches that must include unmerged changes."
                ),
            },
            "max_wait_seconds": {
                "type": "integer",
                "description": "Maximum time to keep watching the AO worker before returning.",
                "default": 1800,
            },
        },
        "required": ["prompt"],
    },
}


def _progress_callback(parent_agent):
    return getattr(parent_agent, "tool_progress_callback", None) if parent_agent else None


def _emit(cb, event_type: str, session: AOSession, goal: str, preview: str = "", **extra) -> None:
    if not cb:
        return
    fields = {
        "subagent_id": f"ao:{session.id}",
        "depth": 0,
        "goal": goal,
        **session.event_fields(),
        **extra,
    }
    cb(event_type, tool_name="ao_delegate_task", preview=preview, **fields)


def _summary_from_output(output: str, limit: int = 500) -> str:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return ""
    text = "\n".join(lines[-6:])
    return text[-limit:]


def _is_separator(line: str) -> bool:
    return line.strip().startswith("──")


def _output_indicates_codex_complete(output: str) -> bool:
    lines = [line.rstrip() for line in output.splitlines()]
    separator_indexes = [idx for idx, line in enumerate(lines) if _is_separator(line)]
    if not separator_indexes:
        return False
    tail = [line.strip() for line in lines[separator_indexes[-1] + 1 :] if line.strip()]
    if any("Working (" in line for line in tail):
        return False
    return any(line.startswith("› ") for line in tail)


def _summary_from_completed_output(output: str, limit: int = 1200) -> str:
    lines = [line.rstrip() for line in output.splitlines()]
    separator_indexes = [idx for idx, line in enumerate(lines) if _is_separator(line)]
    if len(separator_indexes) >= 2:
        start = separator_indexes[-2] + 1
        end = separator_indexes[-1]
        block = lines[start:end]
    else:
        block = lines

    cleaned = []
    for line in block:
        text = line.strip()
        if not text or _is_separator(text):
            continue
        if text.startswith("• "):
            text = text[2:].strip()
        cleaned.append(text)
    return "\n".join(cleaned)[-limit:]


def ao_delegate_task(
    *,
    prompt: str,
    goal: Optional[str] = None,
    project_id: str = "OrynWorkspace",
    issue_id: Optional[str] = None,
    branch: Optional[str] = None,
    max_wait_seconds: int = 1800,
    parent_agent=None,
    bridge: Optional[AOBridge] = None,
) -> str:
    prompt = (prompt or "").strip()
    if not prompt:
        return tool_error("ao_delegate_task requires a non-empty prompt")

    goal = (goal or prompt.splitlines()[0])[:180]
    project_id = (project_id or "OrynWorkspace").strip()
    max_wait_seconds = max(5, min(int(max_wait_seconds or 1800), 7200))
    bridge = bridge or AOBridge()
    cb = _progress_callback(parent_agent)

    try:
        session = bridge.spawn(project_id=project_id, prompt=prompt, issue_id=issue_id, branch=branch)
    except Exception as exc:
        return tool_error(f"AO spawn failed: {exc}")

    _emit(cb, "subagent.start", session, goal, preview=f"AO session {session.id} spawned")

    started = time.monotonic()
    last_signature = None
    pending_complete_summary = None
    final_output = ""

    while time.monotonic() - started < max_wait_seconds:
        try:
            current = bridge.status(session.id) or session
        except AOBridgeError:
            current = session
            current.status = "killed"
        output = bridge.capture_output(current, lines=50)
        final_output = output or final_output
        summary = _summary_from_output(output)
        inferred_complete = _output_indicates_codex_complete(output)
        if inferred_complete and not current.is_terminal:
            completed_summary = _summary_from_completed_output(output) or current.summary or summary
            if completed_summary and completed_summary == pending_complete_summary:
                current.status = "done"
                current.summary = completed_summary
                summary = current.summary or summary
            else:
                pending_complete_summary = completed_summary
                inferred_complete = False
        elif not inferred_complete:
            pending_complete_summary = None
        signature = (current.status, current.activity, summary)

        if signature != last_signature:
            last_signature = signature
            _emit(
                cb,
                "subagent.progress",
                current,
                goal,
                preview=summary or f"AO session {current.id}: {current.status or 'running'}",
                output_tail=[{"tool": "tmux", "preview": summary, "is_error": False}] if summary else [],
            )

        session = current
        if current.is_terminal:
            break
        time.sleep(3)

    timed_out = not session.is_terminal
    result_summary = session.summary or _summary_from_output(final_output, limit=1200)
    final_status = "running" if timed_out else session.display_status

    if timed_out:
        _emit(
            cb,
            "subagent.progress",
            session,
            goal,
            preview=f"AO session {session.id} is still running after {max_wait_seconds}s",
            status="running",
        )
    else:
        _emit(
            cb,
            "subagent.complete",
            session,
            goal,
            preview=result_summary or f"AO session {session.id} finished with {session.status}",
            summary=result_summary or session.summary,
        )

    return tool_result(
        {
            "ok": True,
            "runtime": "ao",
            "status": final_status,
            "timed_out": timed_out,
            "session": session.event_fields(),
            "summary": result_summary or session.summary,
        }
    )


def _handle_ao_delegate_task(args: Dict[str, Any], **kwargs) -> str:
    return ao_delegate_task(
        prompt=args.get("prompt") or args.get("goal") or "",
        goal=args.get("goal"),
        project_id=args.get("project_id") or "OrynWorkspace",
        issue_id=args.get("issue_id"),
        branch=args.get("branch"),
        max_wait_seconds=args.get("max_wait_seconds") or 1800,
        parent_agent=kwargs.get("parent_agent"),
    )


registry.register(
    name="ao_delegate_task",
    toolset="delegation",
    schema=AO_DELEGATE_TASK_SCHEMA,
    handler=_handle_ao_delegate_task,
    emoji="AO",
    max_result_size_chars=20_000,
)
