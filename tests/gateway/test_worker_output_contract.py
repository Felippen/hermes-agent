from __future__ import annotations

import json

from gateway.dev_control.work_case_hooks import create_work_case_for_dispatch
from gateway.dev_control.worker_output_contract import (
    append_worker_output_contract,
    parse_worker_output_contract,
    worker_output_contract_score,
)
from gateway.dev_execution import DevExecutionStore, derive_execution_plan_status
from gateway.dev_worker_runtimes import WorkerRuntimeRouter
from gateway.subagent_events import SubagentEventStore


VALID_OUTPUT = """
Completed the inspection.

```json DEV_WORKER_EVIDENCE
{
  "summary": "Verified runtime metadata decoding.",
  "findings": ["WorkspaceSubagentActivity decodes runtime fields."],
  "files_read": ["apps/oryn-workspace/Sources/OrynWorkspaceCore/Models/WorkspaceSubagentActivity.swift"],
  "files_changed": [],
  "commands_run": ["swift test --filter WorkspaceSubagentActivityTests"],
  "verification": {
    "status": "passed",
    "evidence": ["WorkspaceSubagentActivityTests passed."]
  },
  "unresolved_gaps": [],
  "confidence": 0.86,
  "final_marker": "PHASE26_DONE"
}
```

FINAL_MARKER: PHASE26_DONE
"""

_RUNTIME_FIXTURE = r'''
import json
import time
from pathlib import Path


class WorkCaseRuntime:
    def __init__(self, cases_root=None):
        self.cases_root = Path(cases_root)
        self.cases_root.mkdir(parents=True, exist_ok=True)

    def case_path(self, case_id):
        return self.cases_root / case_id

    def read_json(self, path):
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def _write_json(self, path, payload):
        Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def create_case(self, *, title, summary, dispatch):
        case_id = "wc-test-1"
        root = self.case_path(case_id)
        root.mkdir(parents=True, exist_ok=True)
        self._write_json(root / "case.json", {
            "case_id": case_id,
            "title": title,
            "summary": summary,
            "status": "open",
            "dispatch": dispatch,
        })
        self._write_json(root / "carry_forward.json", {"verification_state": "unknown"})
        (root / "events.jsonl").write_text("", encoding="utf-8")
        return case_id

    def record_event(self, case_id, *, event_type, message):
        with (self.case_path(case_id) / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"type": event_type, "message": message, "created_at": time.time()}) + "\n")

    def update_carry_forward(self, case_id, updates):
        path = self.case_path(case_id) / "carry_forward.json"
        current = self.read_json(path)
        current.update(updates)
        self._write_json(path, current)

    def close_case(self, case_id, *, learnings=None, require_verified=True):
        path = self.case_path(case_id) / "case.json"
        metadata = self.read_json(path)
        metadata["status"] = "closed_verified" if require_verified else "closed_unverified"
        if learnings:
            metadata["learnings"] = learnings
        self._write_json(path, metadata)
'''


def _install_runtime_fixture(tmp_path):
    cases_root = tmp_path / "cases"
    vault_root = tmp_path / "vault"
    oryn_root = tmp_path / "Oryn"
    package = oryn_root / "tools" / "dev_reliability"
    package.mkdir(parents=True)
    (package / "work_case_runtime.py").write_text(_RUNTIME_FIXTURE, encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package.parent / "__init__.py").write_text("", encoding="utf-8")
    return oryn_root, cases_root, vault_root


class _ContractSmokeSession:
    id = "fixture-phase-26-tail"
    status = "running"
    summary = None


class _ContractSmokeBridge:
    def __init__(self, output_tail: str):
        self.session = _ContractSmokeSession()
        self.output_tail = output_tail

    def status(self, *args):
        session_id = args[-1]
        return self.session if session_id == self.session.id else None

    def list(self, *args, project_id=None):
        return [self.session]

    def runtime_health(self, *args):
        return {"runtime_health": "ok", "runtime_warning": None}

    def capture_output(self, *args, lines=40):
        return self.output_tail


def test_worker_output_contract_parser_accepts_valid_markdown_json():
    parsed = parse_worker_output_contract(VALID_OUTPUT)

    assert parsed["output_contract_version"] == 2
    assert parsed["output_contract_status"] == "ok"
    assert parsed["structured_summary"] == "Verified runtime metadata decoding."
    assert parsed["findings"] == ["WorkspaceSubagentActivity decodes runtime fields."]
    assert parsed["files_read"] == ["apps/oryn-workspace/Sources/OrynWorkspaceCore/Models/WorkspaceSubagentActivity.swift"]
    assert parsed["commands_run"] == ["swift test --filter WorkspaceSubagentActivityTests"]
    assert parsed["verification_status"] == "passed"
    assert parsed["verification_evidence"] == ["WorkspaceSubagentActivityTests passed."]
    assert parsed["worker_confidence"] == 0.86
    assert parsed["final_marker"] == "PHASE26_DONE"
    assert worker_output_contract_score(parsed, required_marker="PHASE26_DONE") == 1.0


def test_worker_output_contract_parser_reports_missing_and_invalid_without_crashing():
    missing = parse_worker_output_contract("Finished without structured evidence.")
    invalid = parse_worker_output_contract("```json DEV_WORKER_EVIDENCE\n{\"summary\": \n```")

    assert missing["output_contract_status"] == "warning"
    assert "did not include" in missing["output_contract_warning"]
    assert invalid["output_contract_status"] == "warning"
    assert "no valid JSON object" in invalid["output_contract_warning"]


def test_worker_output_contract_parser_accepts_unknown_and_marks_failed_verification():
    unknown = parse_worker_output_contract(VALID_OUTPUT.replace('"status": "passed"', '"status": "unknown"'))
    failed = parse_worker_output_contract(VALID_OUTPUT.replace('"status": "passed"', '"status": "failed"'))

    assert unknown["output_contract_status"] == "ok"
    assert unknown["verification_status"] == "unknown"
    assert failed["output_contract_status"] == "failed"
    assert failed["verification_status"] == "failed"


def test_worker_output_contract_parser_recovers_plain_marker_json():
    parsed = parse_worker_output_contract("""
Repository is readable.

DEV_WORKER_EVIDENCE

{
  "structured_summary": "Read-only smoke inspection completed.",
  "files_read": ["README.md"],
  "files_changed": [],
  "commands_run": ["git status --short"],
  "verification": {
    "status": "not_run",
    "reason": "Task brief requested read-only file inspection only."
  },
  "deviations_from_brief": []
}
""")

    assert parsed["output_contract_status"] == "warning"
    assert parsed["structured_summary"] == "Read-only smoke inspection completed."
    assert parsed["files_read"] == ["README.md"]
    assert parsed["commands_run"] == ["git status --short"]
    assert parsed["verification_status"] == "not_run"
    assert parsed["verification_evidence"] == ["Task brief requested read-only file inspection only."]


def test_worker_output_contract_parser_repairs_wrapped_json_strings():
    parsed = parse_worker_output_contract('''
DEV_WORKER_EVIDENCE
{
  "summary": "Repository is readable. README.md was inspected successfully.",
  "findings": [
    "README.md documents
    project purpose and quick start."
  ],
  "files_read": ["README.md"],
  "files_changed": [],
  "commands_run": ["sed -n '1,160p' README.md"],
  "verification": {
    "status": "not_run",
    "evidence": ["Read-only README.md inspection completed."]
  },
  "unresolved_gaps": [],
  "confidence": 0.8,
  "final_marker": null
}
''')

    assert parsed["output_contract_status"] == "warning"
    assert "control characters" in parsed["output_contract_warning"]
    assert parsed["structured_summary"] == "Repository is readable. README.md was inspected successfully."
    assert parsed["files_read"] == ["README.md"]
    assert parsed["verification_status"] == "not_run"
    assert parsed["verification_evidence"] == ["Read-only README.md inspection completed."]


def test_worker_output_contract_prompt_helper_is_idempotent():
    prompt = append_worker_output_contract("Do the work.")

    assert "Worker Output Contract v2" in prompt
    assert append_worker_output_contract(prompt) == prompt


def test_subagent_complete_persists_and_status_uses_structured_evidence(tmp_path):
    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="Structured evidence plan",
        vision_brief=None,
        tasks=[{
            "goal": "Return PHASE26_DONE",
            "prompt": "Inspect and return PHASE26_DONE.",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="fixture-phase-26",
    )

    event = event_store.append_event({
        "event": "subagent.complete",
        "subagent_id": "fixture:phase-26",
        "ao_session_id": "fixture-phase-26",
        "runtime": "fixture",
        "status": "completed",
        "summary": VALID_OUTPUT,
        "goal": "Return PHASE26_DONE",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })
    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        event_store=event_store,
    )

    task_status = status["tasks"][0]
    assert event["output_contract_status"] == "ok"
    assert status["status"] == "completed"
    assert task_status["summary"] == "Verified runtime metadata decoding."
    assert task_status["summary_quality"] == "ok"
    assert task_status["output_contract_status"] == "ok"
    assert task_status["files_read"] == ["apps/oryn-workspace/Sources/OrynWorkspaceCore/Models/WorkspaceSubagentActivity.swift"]
    assert "WorkspaceSubagentActivityTests passed." in task_status["verification_evidence"]


def test_status_derivation_does_not_close_work_case_as_verified_from_worker_output(tmp_path, monkeypatch):
    oryn_root, cases_root, vault_root = _install_runtime_fixture(tmp_path)
    monkeypatch.setenv("ORYN_ROOT", str(oryn_root))
    monkeypatch.setenv("ORYN_WORK_CASE_HOME", str(cases_root))
    monkeypatch.setenv("HERMES_VAULT_ROOT", str(vault_root))
    monkeypatch.setenv("HERMES_DEV_WORK_CASE_AUTO", "1")

    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="Work Case hook plan",
        vision_brief=None,
        tasks=[{
            "goal": "Return PHASE26_DONE",
            "prompt": "Inspect and return PHASE26_DONE.",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="fixture-work-case-hook",
    )
    case_id = create_work_case_for_dispatch(
        plan_id=plan["plan_id"],
        task=task,
        ao_session_id="fixture-work-case-hook",
        runtime="fixture",
        project_id="OrynWorkspace",
    )
    assert case_id
    store.patch_task_payload(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        payload_updates={"work_case_id": case_id},
    )

    event_store.append_event({
        "event": "subagent.complete",
        "subagent_id": "fixture:work-case-hook",
        "ao_session_id": "fixture-work-case-hook",
        "runtime": "fixture",
        "status": "completed",
        "summary": VALID_OUTPUT,
        "goal": "Return PHASE26_DONE",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })

    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        event_store=event_store,
    )

    assert status["tasks"][0]["work_case_closed"] is True
    metadata = json.loads((cases_root / case_id / "case.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "closed_unverified"
    carry = json.loads((cases_root / case_id / "carry_forward.json").read_text(encoding="utf-8"))
    assert carry["verification_state"] == "unknown"


def test_missing_structured_evidence_is_warning_first_for_good_summary(tmp_path):
    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="Warning-first plan",
        vision_brief=None,
        tasks=[{
            "goal": "Inspect without marker",
            "prompt": "Inspect without marker.",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="fixture-phase-26-warning",
    )
    event_store.append_event({
        "event": "subagent.complete",
        "subagent_id": "fixture:phase-26-warning",
        "ao_session_id": "fixture-phase-26-warning",
        "runtime": "fixture",
        "status": "completed",
        "summary": "Verified the relevant behavior with concrete evidence and no unresolved gaps.",
        "goal": "Inspect without marker",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })

    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        event_store=event_store,
    )

    task_status = status["tasks"][0]
    assert status["status"] == "completed"
    assert task_status["summary_quality"] == "ok"
    assert task_status["summary_warning"] is None
    assert task_status["output_contract_status"] == "warning"
    assert "DEV_WORKER_EVIDENCE" in task_status["output_contract_warning"]


def test_structured_unresolved_gaps_make_completed_task_reviewable(tmp_path):
    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="Structured gaps plan",
        vision_brief=None,
        tasks=[{
            "goal": "Return PHASE26_DONE",
            "prompt": "Inspect and return PHASE26_DONE.",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="fixture-phase-26-gaps",
    )

    event_store.append_event({
        "event": "subagent.complete",
        "subagent_id": "fixture:phase-26-gaps",
        "ao_session_id": "fixture-phase-26-gaps",
        "runtime": "fixture",
        "status": "completed",
        "summary": VALID_OUTPUT.replace('"unresolved_gaps": []', '"unresolved_gaps": ["Need product owner confirmation."]'),
        "goal": "Return PHASE26_DONE",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })
    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        event_store=event_store,
    )

    task_status = status["tasks"][0]
    assert status["status"] == "needs_review"
    assert task_status["summary_warning"] == "Worker reported unresolved gaps in structured evidence."
    assert task_status["unresolved_gaps"] == ["Need product owner confirmation."]


def test_status_reparses_runtime_tail_when_persisted_contract_is_weak(tmp_path):
    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="Weak event reparsed from runtime tail",
        vision_brief=None,
        tasks=[{
            "goal": "Inspect README.md",
            "prompt": "Inspect README.md.",
            "runtime": "ao",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="fixture-phase-26-tail",
    )
    event_store.append_event({
        "event": "subagent.complete",
        "subagent_id": "ao:fixture-phase-26-tail",
        "ao_session_id": "fixture-phase-26-tail",
        "runtime": "ao",
        "status": "completed",
        "summary": "I will inspect README.md.",
        "goal": "Inspect README.md",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
        "output_contract_version": 2,
        "output_contract_status": "warning",
        "output_contract_warning": "Worker evidence block marker was present but no valid JSON object could be extracted.",
        "output_contract_score": 0.2,
    })
    bridge = _ContractSmokeBridge("""
DEV_WORKER_EVIDENCE
{
  "summary": "Repository is readable. README.md was inspected successfully.",
  "findings": [
    "README.md documents
    project purpose and quick start."
  ],
  "files_read": ["README.md"],
  "files_changed": [],
  "commands_run": ["nl -ba README.md"],
  "verification": {
    "status": "not_run",
    "evidence": ["Read-only README.md inspection completed."]
  },
  "unresolved_gaps": [],
  "confidence": 0.8,
  "final_marker": null
}
""")

    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        bridge=bridge,
        event_store=event_store,
    )

    task_status = status["tasks"][0]
    assert task_status["output_contract_status"] == "warning"
    assert "control characters" in task_status["output_contract_warning"]
    assert task_status["structured_summary"] == "Repository is readable. README.md was inspected successfully."
    assert task_status["files_read"] == ["README.md"]
    assert task_status["commands_run"] == ["nl -ba README.md"]
    assert task_status["verification_status"] == "not_run"
    assert task_status["verification_evidence"] == ["Read-only README.md inspection completed."]
    assert task_status["output_contract_score"] == 0.75


def test_openhands_running_task_completes_from_terminal_contract_evidence(tmp_path):
    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="OpenHands terminal evidence plan",
        vision_brief=None,
        tasks=[{
            "goal": "Inspect README.md",
            "prompt": "Inspect README.md.",
            "runtime": "openhands",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="oh-contract-running",
    )
    event_store.append_event({
        "event": "subagent.start",
        "subagent_id": "openhands:oh-contract-running",
        "runtime": "openhands",
        "runtime_session_id": "oh-contract-running",
        "runtime_project_id": "OrynWorkspace",
        "status": "running",
        "summary": "OpenHands session started.",
        "goal": "Inspect README.md",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })
    bridge = _ContractSmokeBridge("""
DEV_WORKER_EVIDENCE
{
  "summary": "Repository inspection completed successfully.",
  "findings": "README.md is present and readable.",
  "files_read": ["README.md"],
  "files_changed": [],
  "commands_run": [],
  "verification": {
    "status": "not_run",
    "evidence": ["Read-only README.md inspection completed."]
  },
  "unresolved_gaps": [],
  "confidence": "high",
  "final_marker": "---END---"
}
""")
    bridge.session.id = "oh-contract-running"

    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        bridge=WorkerRuntimeRouter(openhands_bridge=bridge),
        event_store=event_store,
    )

    task_status = status["tasks"][0]
    assert status["status"] == "completed"
    assert status["review_status"] == "accepted"
    assert task_status["status"] == "completed"
    assert task_status["status_reason"] == "OpenHands emitted terminal structured evidence."
    assert task_status["structured_summary"] == "Repository inspection completed successfully."
    assert task_status["files_read"] == ["README.md"]
    assert task_status["verification_evidence"] == ["Read-only README.md inspection completed."]
    assert task_status["output_contract_status"] == "warning"
    assert task_status["output_contract_score"] == 0.75
    terminal_events = [
        event
        for event in event_store.list_events(subagent_id="openhands:oh-contract-running", limit=10)
        if event["event"] == "subagent.complete"
    ]
    assert len(terminal_events) == 1
    assert terminal_events[0]["status"] == "completed"
    assert terminal_events[0]["contract_inferred_completion"] is True
    assert terminal_events[0]["summary"] == "Repository inspection completed successfully."


def test_openhands_failed_verification_contract_marks_task_failed(tmp_path):
    store = DevExecutionStore(tmp_path / "state.db")
    event_store = SubagentEventStore(tmp_path / "state.db")
    plan = store.create_plan(
        title="OpenHands failed evidence plan",
        vision_brief=None,
        tasks=[{
            "goal": "Verify README.md",
            "prompt": "Verify README.md.",
            "runtime": "openhands",
        }],
    )
    task = plan["tasks"][0]
    store.update_task_launch(
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        ao_session_id="oh-contract-failed",
    )
    event_store.append_event({
        "event": "subagent.start",
        "subagent_id": "openhands:oh-contract-failed",
        "runtime": "openhands",
        "runtime_session_id": "oh-contract-failed",
        "runtime_project_id": "OrynWorkspace",
        "status": "running",
        "summary": "OpenHands session started.",
        "goal": "Verify README.md",
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })
    bridge = _ContractSmokeBridge(VALID_OUTPUT.replace('"status": "passed"', '"status": "failed"'))
    bridge.session.id = "oh-contract-failed"

    status = derive_execution_plan_status(
        store=store,
        plan_id=plan["plan_id"],
        bridge=WorkerRuntimeRouter(openhands_bridge=bridge),
        event_store=event_store,
    )

    task_status = status["tasks"][0]
    assert status["status"] == "failed"
    assert task_status["status"] == "failed"
    assert task_status["status_reason"] == "OpenHands emitted failed verification evidence."
    assert task_status["output_contract_status"] == "failed"
    terminal_events = [
        event
        for event in event_store.list_events(subagent_id="openhands:oh-contract-failed", limit=10)
        if event["event"] == "subagent.complete"
    ]
    assert len(terminal_events) == 1
    assert terminal_events[0]["status"] == "failed"
    assert terminal_events[0]["contract_inferred_failure"] is True
