"""Auto-create and close Dev Work Cases when execution tasks dispatch."""

from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

TERMINAL_TASK_STATUSES = {
    "completed",
    "needs_review",
    "failed",
    "cancelled",
    "done",
    "merged",
    "complete",
    "success",
    "succeeded",
    "completed_with_errors",
}


def work_case_auto_enabled() -> bool:
    return os.environ.get("HERMES_DEV_WORK_CASE_AUTO", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _oryn_tools_path() -> Optional[Path]:
    configured = os.environ.get("ORYN_ROOT", "").strip()
    if configured:
        candidate = Path(configured).expanduser() / "tools"
        if (candidate / "dev_reliability" / "work_case_runtime.py").exists():
            return candidate
    hermes_root = Path(__file__).resolve().parents[2]
    candidate = hermes_root.parent / "tools"
    if (candidate / "dev_reliability" / "work_case_runtime.py").exists():
        return candidate
    return None


def _load_runtime():
    tools_path = _oryn_tools_path()
    if tools_path is None:
        return None
    tools_str = str(tools_path)
    if tools_str in sys.path:
        sys.path.remove(tools_str)
    sys.path.insert(0, tools_str)
    for module_name in list(sys.modules):
        if module_name == "dev_reliability" or module_name.startswith("dev_reliability."):
            del sys.modules[module_name]
    runtime_module = importlib.import_module("dev_reliability.work_case_runtime")
    WorkCaseRuntime = runtime_module.WorkCaseRuntime

    cases_root = os.environ.get("ORYN_WORK_CASE_HOME", "").strip()
    if cases_root:
        return WorkCaseRuntime(cases_root=Path(cases_root))
    return WorkCaseRuntime()


def create_work_case_for_dispatch(
    *,
    plan_id: str,
    task: dict[str, Any],
    ao_session_id: str,
    runtime: str,
    project_id: Optional[str],
) -> Optional[str]:
    if not work_case_auto_enabled():
        return None
    try:
        work_case = _load_runtime()
        if work_case is None:
            logger.debug("Work Case runtime unavailable; skipping auto-create")
            return None
        goal = str(task.get("goal") or task.get("task_id") or "Dev task").strip()
        case_id = work_case.create_case(
            title=goal[:120] or "Dev dispatch",
            summary=str(task.get("prompt") or "").strip()[:2000],
            dispatch={
                "plan_id": plan_id,
                "task_id": task.get("task_id"),
                "project_id": project_id,
                "ao_session_id": ao_session_id,
                "runtime": runtime,
            },
        )
        work_case.record_event(
            case_id,
            event_type="dispatch",
            message=f"Worker dispatched for plan {plan_id} task {task.get('task_id')} via {runtime}.",
        )
        return case_id
    except Exception as exc:
        logger.warning("Work Case auto-create failed: %s", exc)
        return None


def maybe_close_work_case_for_task(
    *,
    task: dict[str, Any],
    derived: dict[str, Any],
    store: Any = None,
) -> None:
    if not work_case_auto_enabled():
        return
    payload = task.get("payload") or {}
    case_id = str(payload.get("work_case_id") or "").strip()
    if not case_id:
        return
    status = str(derived.get("derived_status") or derived.get("status") or "").strip().lower()
    if status not in TERMINAL_TASK_STATUSES:
        return
    try:
        work_case = _load_runtime()
        if work_case is None:
            return
        case_root = work_case.case_path(case_id)
        if case_root.exists():
            metadata = work_case.read_json(case_root / "case.json")
            if str(metadata.get("status") or "").startswith("closed"):
                return
        summary = str(derived.get("summary") or derived.get("status_reason") or "").strip()
        evidence = derived.get("verification_evidence") or []
        evidence_text = "\n".join(str(item).strip() for item in evidence if str(item).strip())
        learnings_parts = [part for part in (summary, evidence_text) if part]
        learnings = "\n\n".join(learnings_parts).strip()
        if learnings:
            work_case.update_carry_forward(case_id, {"summary": summary, "learnings": learnings})
        verification_state = "passed" if status in {"completed", "done", "merged", "complete", "success", "succeeded"} else "unknown"
        if evidence_text:
            work_case.record_verify(
                case_id,
                tier="L1",
                command="worker_output_contract",
                outcome="passed" if verification_state == "passed" else "unknown",
                evidence=evidence_text[:4000],
            )
        work_case.record_event(
            case_id,
            event_type="task_terminal",
            message=f"Task reached terminal status {status}.",
        )
        work_case.close_case(case_id, learnings=learnings or None, require_verified=False)
        derived["work_case_id"] = case_id
        derived["work_case_closed"] = True
        plan_id = str(task.get("plan_id") or "").strip()
        task_id = str(task.get("task_id") or "").strip()
        if store is not None and plan_id and task_id:
            import time

            store.patch_task_payload(
                plan_id=plan_id,
                task_id=task_id,
                payload_updates={"work_case_closed_at": time.time()},
            )
    except Exception as exc:
        logger.warning("Work Case auto-close failed for %s: %s", case_id, exc)
