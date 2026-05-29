from __future__ import annotations

import os
import pytest
import shutil
import subprocess
from pathlib import Path

from gateway.dev_control.dogfood_backlog import dogfood_scope_check
from gateway.dev_control.lab_process_isolation import audit_process_isolation
from gateway.dev_control.lab_loop import DevLabLoopStore, _await_implementation_terminal, _touched_paths_from_worktree, loop_health, run_lab_loop_pass
from gateway.dev_control.reliability import DevReliabilityStore, scorecard
from gateway.subagent_events import SubagentEventStore
from gateway.dev_execution import DevExecutionStore
from scripts.seed_dev_lab_data import seed_lab_data
from tools.ao_bridge import AOSession


def _env(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    lab_home = tmp_path / "lab"
    db_path = lab_home / "hermes-home" / "state.db"
    stable_db = tmp_path / "stable" / "state.db"
    stable_db.parent.mkdir(parents=True)
    stable_db.write_text("stable", encoding="utf-8")
    (lab_home / "repos" / "hermes-agent").mkdir(parents=True)
    (lab_home / "repos" / "Oryn").mkdir(parents=True)
    (lab_home / "worktrees").mkdir(parents=True)
    monkeypatch.setenv("ORYN_LAB_HOME", str(lab_home))
    monkeypatch.setenv("HERMES_HOME", str(lab_home / "hermes-home"))
    monkeypatch.setenv("API_SERVER_PORT", "8662")
    monkeypatch.delenv("HERMES_DEV_MERGE_EXECUTOR_ENABLED", raising=False)
    monkeypatch.delenv("HERMES_DEV_BRANCH_PROTECTION_CONFIRMED", raising=False)
    monkeypatch.setenv("HERMES_DEV_LAB_MIN_TERMINAL_SECONDS", "0")
    monkeypatch.setattr(
        "gateway.dev_control.lab_loop.audit_current_process_isolation",
        lambda extra_pids=None: {
            "ok": True,
            "object": "hermes.dev_lab_process_isolation",
            "pids": [os.getpid(), *(extra_pids or [])],
            "write_handles": [],
            "offending_paths": [],
            "warnings": [],
            "authoritative": True,
        },
    )
    return db_path, stable_db


class _FakeLabRouter:
    def __init__(
        self,
        lab_home: Path,
        *,
        diff_paths: list[str] | None = None,
        status: str = "done",
        status_sequence: list[str] | None = None,
        transcript: str | None = None,
        spawn_error: Exception | None = None,
    ):
        self.lab_home = lab_home
        self.diff_paths = diff_paths or []
        self.status_value = status
        self.status_sequence = list(status_sequence or [])
        self.transcript = transcript
        self.spawn_error = spawn_error
        self.spawned = []
        self.sessions: dict[str, AOSession] = {}

    def spawn(self, *args, **kwargs):
        if self.spawn_error:
            raise self.spawn_error
        index = len(self.spawned) + 1
        workspace = self.lab_home / "worktrees" / f"dogfood-{index}"
        _init_git_repo(workspace)
        for rel in self.diff_paths:
            path = workspace / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"lab change for {rel}\n", encoding="utf-8")
        session = AOSession(
            id=f"lab-session-{index}",
            project_id=kwargs.get("project_id"),
            status=self.status_value,
            branch=kwargs.get("branch"),
            workspace_path=str(workspace),
            agent=kwargs.get("agent") or "codex",
            model=kwargs.get("model") or "gpt-5.5",
            reasoning_effort=kwargs.get("reasoning_effort"),
            summary="PHASE16_FIXTURE_OK_DONE Completed the lab dogfood task with scoped evidence.",
        )
        self.spawned.append({"args": args, "kwargs": kwargs, "session": session})
        self.sessions[session.id] = session
        return session

    def status(self, *args):
        session_id = args[-1]
        session = self.sessions.get(session_id)
        if session and self.status_sequence:
            session.status = self.status_sequence.pop(0)
        return session

    def list(self, *args, **kwargs):
        return list(self.sessions.values())

    def runtime_health(self, *args):
        return {"runtime_health": "ok", "runtime_warning": None}

    def capture_output(self, *args, **kwargs):
        return self.transcript or "PHASE16_FIXTURE_OK_DONE Completed the lab dogfood task with scoped evidence."


def _init_git_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "lab@example.test"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Hermes Lab"], cwd=path, check=True)
    (path / "README.md").write_text("seed\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "seed"], cwd=path, check=True)


def test_preapproved_dogfood_pass_writes_real_outcome_and_keeps_stable_db(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    store = DevLabLoopStore(db_path)
    candidate = store.upsert_candidate({
        "prompt": "Add a small docs note for lab dogfood.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab-dogfood.md"],
        "source": "docs",
    }, approved=True)
    before = stable_db.stat().st_mtime

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        sources=["reliability"],
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["docs/lab-dogfood.md"]),
    )

    assert report["status"] == "completed"
    assert report["isolation"]["ok"] is True
    assert report["stable_db_telemetry"]["authoritative"] is False
    assert report["candidate_id"] == candidate["candidate_id"]
    assert report["stable_db_unchanged"] is True
    assert stable_db.stat().st_mtime == before
    outcomes = DevReliabilityStore(db_path).list_outcomes(limit=20)
    assert outcomes
    assert outcomes[0]["source_refs"]["source"] == "dogfood_lab_loop"
    assert outcomes[0]["source_refs"]["seeded"] is False
    assert outcomes[0]["source_refs"]["implement_session_id"] == "lab-session-1"
    assert outcomes[0]["source_refs"]["draft_pr_only"] is True
    assert outcomes[0]["ci_state"] == "unknown"
    assert outcomes[0]["code_review_verdict"] == "unknown"
    assert loop_health(db_path=db_path)["real_outcome_count"] == 1


def test_stable_db_mtime_change_is_informational_not_a_gate(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Exercise mtime telemetry.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
    }, approved=True)

    def _executor(_candidate, _context):
        stable_db.write_text("stable changed by unrelated live service", encoding="utf-8")
        return {"status": "completed", "duration_seconds": 0.1}

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        executor=_executor,
        max_consecutive_failures=10,
    )

    assert report["status"] == "completed"
    assert report["stable_db_unchanged"] is False
    assert report["stable_db_telemetry"]["authoritative"] is False
    assert report["isolation"]["ok"] is True


def test_open_file_isolation_audit_flags_non_lab_write_handle(monkeypatch, tmp_path):
    if not shutil.which("lsof"):
        pytest.skip("lsof is required for open-file isolation audit")
    db_path, stable_db = _env(monkeypatch, tmp_path)
    outside = tmp_path / "outside-stable" / "state.db"
    outside.parent.mkdir(parents=True)
    handle = outside.open("w", encoding="utf-8")
    try:
        handle.write("open")
        handle.flush()
        audit = audit_process_isolation(pids=[os.getpid()])
    finally:
        handle.close()

    assert not audit["ok"]
    assert any(str(outside.resolve(strict=False)) == item["path"] for item in audit["offending_paths"])


def test_lab_pass_hard_stops_on_non_lab_write_handle(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    store = DevLabLoopStore(db_path)
    store.upsert_candidate({
        "prompt": "Exercise isolation breaker.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
    }, approved=True)
    outside = tmp_path / "outside-stable" / "state.db"
    outside.parent.mkdir(parents=True)
    monkeypatch.setattr(
        "gateway.dev_control.lab_loop.audit_current_process_isolation",
        lambda extra_pids=None: {
            "ok": False,
            "object": "hermes.dev_lab_process_isolation",
            "pids": [os.getpid()],
            "write_handles": [{"pid": os.getpid(), "fd": "9u", "type": "REG", "path": str(outside)}],
            "offending_paths": [{"pid": os.getpid(), "fd": "9u", "type": "REG", "path": str(outside)}],
            "warnings": [],
            "authoritative": True,
        },
    )

    def _executor(_candidate, _context):
        return {"status": "completed", "duration_seconds": 0.1}

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        executor=_executor,
        max_consecutive_failures=10,
    )

    assert report["status"] == "loop_halted"
    assert report["breaker_reason"] == "isolation_breach"
    assert not report["isolation"]["ok"]
    assert store.get_state()["status"] == "halted"


def test_lab_executor_derives_verified_outcome_from_measured_verification(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Measure a passing verification fixture.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/lab_loop.py"],
        "source": "todo",
        "payload": {
            "verification_results": [{
                "criterion_id": "crit-1",
                "status": "passed",
                "command_run": "make test",
                "exit_code": 0,
                "output_excerpt": "1 passed in 0.1s",
            }],
        },
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        sources=["reliability"],
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["gateway/dev_control/lab_loop.py"]),
    )

    assert report["status"] == "completed"
    outcome = DevReliabilityStore(db_path).list_outcomes(limit=1)[0]
    assert outcome["verification_verdict"] == "verified"
    assert outcome["merged"] is False
    assert outcome["source_refs"]["draft_pr_only"] is True
    assert outcome["success"] is False

    ready_payload = dict(outcome)
    ready_payload.pop("outcome_id", None)
    ready = DevReliabilityStore(db_path).upsert_outcome({
        **ready_payload,
        "plan_id": "ready-plan",
        "task_id": "ready-task",
        "terminal_status": "completed",
        "verification_verdict": "verified",
        "ci_state": "success",
        "code_review_verdict": "approved",
        "source_refs": {**outcome["source_refs"], "draft_pr_ready": True},
    })
    assert ready["success"] is True


def test_lab_executor_can_produce_bad_score_from_failed_verification(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Measure a failing verification fixture.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/lab_loop.py"],
        "source": "todo",
        "payload": {
            "verification_results": [{
                "criterion_id": "crit-1",
                "status": "failed",
                "command_run": "make test",
                "exit_code": 1,
                "output_excerpt": "1 failed in 0.1s",
            }],
        },
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        sources=["reliability"],
        max_consecutive_failures=10,
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["gateway/dev_control/lab_loop.py"]),
    )

    assert report["status"] == "failed"
    outcome = DevReliabilityStore(db_path).list_outcomes(limit=1)[0]
    assert outcome["verification_verdict"] == "failed"
    assert outcome["success"] is False


def test_out_of_scope_engine_task_is_skipped_not_failed(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    store = DevLabLoopStore(db_path)
    store.upsert_candidate({
        "prompt": "Modify the conversation loop.",
        "profile_id": "platform.implement",
        "risk_level": "high",
        "target_paths": ["agent/conversation_loop.py"],
        "source": "manual",
    }, approved=True)

    report = run_lab_loop_pass(db_path=db_path, stable_db_path=stable_db, max_consecutive_out_of_scope=10)

    assert report["status"] == "skipped"
    assert report["skip_reason"] == "out_of_scope"
    assert DevReliabilityStore(db_path).list_outcomes(limit=20) == []


def test_circuit_breaker_halts_on_consecutive_failure(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    store = DevLabLoopStore(db_path)
    store.upsert_candidate({
        "prompt": "Exercise failure breaker.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/lab_loop.py"],
        "source": "todo",
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        executor=lambda _candidate, _context: {"status": "failed", "error": "fixture"},
        max_consecutive_failures=1,
    )

    assert report["status"] == "loop_halted"
    assert report["breaker_reason"] == "consecutive_failures:1"
    assert store.get_state()["status"] == "halted"


def test_circuit_breaker_halts_on_cost_budget(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    store = DevLabLoopStore(db_path)
    store.upsert_candidate({
        "prompt": "Exercise cost breaker.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/lab_loop.py"],
        "source": "todo",
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        executor=lambda _candidate, _context: {"status": "completed", "cost_usd": 3.0},
        max_cost_usd=1.0,
    )

    assert report["status"] == "loop_halted"
    assert report["breaker_reason"] == "cost_budget_exceeded:3.0000"


def test_seeded_data_remains_distinguishable_from_real_outcomes(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    seed = seed_lab_data(db_path)
    assert seed["ok"] is True
    assert all((item["source_refs"] or {}).get("seeded") is True for item in DevReliabilityStore(db_path).list_outcomes(limit=20))
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Add docs dogfood.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
    }, approved=True)

    run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        sources=["reliability"],
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["docs/lab.md"]),
    )

    health = loop_health(db_path=db_path)
    assert health["real_outcome_count"] == 1
    assert health["scorecard_summary"]["sample_count"] >= 4


def test_invalid_lab_outcomes_are_excluded_from_scorecard(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    store = DevReliabilityStore(db_path)
    store.upsert_outcome({
        "outcome_id": "devrel-out-042c159df0",
        "plan_id": "bad-plan",
        "task_id": "bad-task",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "terminal_status": "failed",
        "verification_verdict": "unknown",
        "ci_state": "unknown",
        "code_review_verdict": "unknown",
        "source_refs": {"source": "dogfood_lab_loop", "draft_pr_only": True},
    })
    store.upsert_outcome({
        "outcome_id": "devrel-out-valid",
        "plan_id": "valid-plan",
        "task_id": "valid-task",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "terminal_status": "failed",
        "verification_verdict": "unknown",
        "ci_state": "unknown",
        "code_review_verdict": "unknown",
        "source_refs": {"source": "dogfood_lab_loop", "draft_pr_only": True},
    })

    card = scorecard(store.list_outcomes(limit=20))
    health = loop_health(db_path=db_path)

    assert card["summary"]["sample_count"] == 1
    assert health["real_outcome_count"] == 1


def test_runner_invalid_execution_does_not_write_scorecard_outcome(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Runner abort fixture.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
    }, approved=True)

    def _executor(_candidate, _context):
        return {
            "status": "runner_aborted",
            "reason": "runner_defect:premature_terminal",
            "invalid_outcome": True,
            "scorecard_excluded": True,
        }

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        executor=_executor,
        max_consecutive_failures=10,
    )

    assert report["status"] == "failed"
    assert report["outcome_id"] is None
    assert DevReliabilityStore(db_path).list_outcomes(limit=20) == []


def test_scope_filter_rejects_engine_paths():
    assert dogfood_scope_check(["gateway/dev_control/lab_loop.py"])["ok"] is True
    rejected = dogfood_scope_check(["agent/conversation_loop.py"])
    assert rejected["ok"] is False
    assert rejected["status"] == "out_of_scope"


def test_lab_executor_dispatches_worker_and_records_diff_artifact(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    store = DevLabLoopStore(db_path)
    store.upsert_candidate({
        "prompt": "Make a scoped dev_control change.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/lab_loop.py"],
        "source": "todo",
        "payload": {
            "verification_results": [{
                "criterion_id": "crit-1",
                "status": "passed",
                "command_run": "make test",
                "exit_code": 0,
                "output_excerpt": "1 passed in 0.1s",
            }],
        },
    }, approved=True)
    router = _FakeLabRouter(tmp_path / "lab", diff_paths=["gateway/dev_control/lab_loop.py"])

    report = run_lab_loop_pass(db_path=db_path, stable_db_path=stable_db, bridge=router, sources=["reliability"])

    assert report["status"] == "completed"
    assert router.spawned
    assert router.spawned[0]["kwargs"]["project_id"] == "HermesAgentLab"
    assert router.spawned[0]["kwargs"]["branch"].startswith("lab/dogfood/")
    assert report["implement_session_id"] == "lab-session-1"
    assert report["diff_scope"]["status"] == "in_scope"
    assert report["draft_artifact"]["type"] == "local_branch"
    assert not Path(router.sessions["lab-session-1"].workspace_path).exists()


def test_lab_await_uses_authoritative_terminal_not_transcript_inference(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    monkeypatch.setenv("HERMES_DEV_LAB_WORKER_TIMEOUT_SECONDS", "0.01")
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Transcript inference guard",
        vision_brief="Do not accept transcript-only completion.",
        tasks=[{
            "goal": "Guard terminal detection.",
            "prompt": "Guard terminal detection.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-inferred")
    event_store.append_event({
        "event": "subagent.complete",
        "ao_session_id": "lab-session-inferred",
        "subagent_id": "ao:lab-session-inferred",
        "status": "completed",
        "message": "Starting MCP servers (0/7): codex_apps",
        "transcript_inferred_completion": True,
        "launch_plan_id": plan["plan_id"],
        "launch_task_id": task["task_id"],
    })
    router = _FakeLabRouter(tmp_path / "lab", status="running")
    router.sessions["lab-session-inferred"] = AOSession(
        id="lab-session-inferred",
        project_id="HermesAgentLab",
        status="running",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "inferred"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=0.01,
    )

    assert terminal["timed_out"] is True
    assert terminal["authoritative_terminal"] is False


def test_lab_await_waits_for_runtime_terminal_status(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Runtime terminal",
        vision_brief="Wait until the runtime reports done.",
        tasks=[{
            "goal": "Guard runtime terminal detection.",
            "prompt": "Guard runtime terminal detection.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-terminal")
    router = _FakeLabRouter(tmp_path / "lab", status_sequence=["running", "running", "done"])
    router.sessions["lab-session-terminal"] = AOSession(
        id="lab-session-terminal",
        project_id="HermesAgentLab",
        status="running",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "terminal"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=2.0,
    )

    assert terminal.get("timed_out") is not True
    assert terminal["authoritative_terminal"] is True
    assert terminal["status"] == "completed"


def test_lab_await_rejects_implausibly_early_completed_session(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    monkeypatch.setenv("HERMES_DEV_LAB_MIN_TERMINAL_SECONDS", "60")
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Minimum liveness guard",
        vision_brief="Do not accept an immediate completed status.",
        tasks=[{
            "goal": "Guard terminal floor.",
            "prompt": "Guard terminal floor.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-too-fast")
    router = _FakeLabRouter(tmp_path / "lab", status="done")
    router.sessions["lab-session-too-fast"] = AOSession(
        id="lab-session-too-fast",
        project_id="HermesAgentLab",
        status="done",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "too-fast"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=0.01,
    )

    assert terminal["timed_out"] is True
    assert terminal["reason"] == "worker_timeout:0.0s"


def test_lab_await_accepts_valid_worker_evidence_transcript(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Transcript evidence terminal",
        vision_brief="Accept valid direct AO worker evidence.",
        tasks=[{
            "goal": "Append a docs note.",
            "prompt": "Append a docs note.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
            "target_paths": ["docs/lab-dogfood-supervised.md"],
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-evidence")
    transcript = """Worker finished.
```json DEV_WORKER_EVIDENCE
{
  "summary": "Added the lab dogfood supervised note.",
  "findings": ["Commit changes only the requested docs file."],
  "files_read": ["docs/lab-dogfood-supervised.md"],
  "files_changed": ["docs/lab-dogfood-supervised.md"],
  "commands_run": ["git show --stat HEAD"],
  "verification": {
    "status": "passed",
    "evidence": ["git show --stat reports one docs file changed."]
  },
  "unresolved_gaps": [],
  "confidence": 0.92,
  "final_marker": null
}
```
"""
    router = _FakeLabRouter(tmp_path / "lab", status="running", transcript=transcript)
    router.sessions["lab-session-evidence"] = AOSession(
        id="lab-session-evidence",
        project_id="HermesAgentLab",
        status="running",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "evidence"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=2.0,
    )

    assert terminal["authoritative_terminal"] is True
    assert terminal["status"] == "completed"
    assert terminal["task"]["files_changed"] == ["docs/lab-dogfood-supervised.md"]
    events = event_store.list_events(ao_session_id="lab-session-evidence", limit=20)
    assert any(event.get("transcript_evidence_completion") for event in events)


def test_lab_await_accepts_unfenced_worker_evidence_transcript(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Unfenced transcript evidence terminal",
        vision_brief="Accept direct AO worker evidence rendered without fences.",
        tasks=[{
            "goal": "Append a docs note.",
            "prompt": "Append a docs note.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
            "target_paths": ["docs/lab-dogfood-supervised.md"],
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-unfenced")
    transcript = """
{
  "summary": "Added the one-line dated lab dogfood verification note at the requested docs path and committed it.",
  "findings": [
    "Commit e941a7056 changes only docs/lab-dogfood-supervised.md."
  ],
  "files_read": [
    "docs/lab-dogfood-supervised.md"
  ],
  "files_changed": [
    "docs/lab-dogfood-supervised.md"
  ],
  "commands_run": [
    "git diff-tree --no-commit-id --name-only -r HEAD"
  ],
  "verification": {
    "status": "passed",
    "evidence": [
      "git diff-tree returned only docs/lab-dogfood-supervised.md."
    ]
  },
  "unresolved_gaps": [],
  "confidence": 0.97,
  "final_marker": null
}
"""
    router = _FakeLabRouter(tmp_path / "lab", status="running", transcript=transcript)
    router.sessions["lab-session-unfenced"] = AOSession(
        id="lab-session-unfenced",
        project_id="HermesAgentLab",
        status="running",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "unfenced"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=2.0,
    )

    assert terminal["authoritative_terminal"] is True
    assert terminal["status"] == "completed"
    assert terminal["task"]["files_changed"] == ["docs/lab-dogfood-supervised.md"]


def test_lab_await_ignores_prompt_template_before_actual_evidence(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Prompt template then actual evidence",
        vision_brief="Accept actual evidence after the prompt template.",
        tasks=[{
            "goal": "Append a docs note.",
            "prompt": "Append a docs note.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
            "target_paths": ["docs/lab-dogfood-supervised.md"],
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-template-then-real")
    transcript = """
```json DEV_WORKER_EVIDENCE
{
  "summary": "What you concluded or changed.",
  "findings": ["Concrete finding or result."],
  "files_read": ["path/or/file.ext"],
  "files_changed": [],
  "commands_run": ["command --if-any"],
  "verification": {"status": "passed", "evidence": ["What proves the result."]},
  "unresolved_gaps": [],
  "confidence": 0.86,
  "final_marker": null
}
```
{
  "summary": "Added the requested one-line dated lab dogfood note and committed only docs/lab-dogfood-supervised.md.",
  "findings": ["No Hermes engine files were touched."],
  "files_read": ["docs/lab-dogfood-supervised.md"],
  "files_changed": ["docs/lab-dogfood-supervised.md"],
  "commands_run": ["pytest tests/gateway/test_api_server_runs.py -q"],
  "verification": {"status": "passed", "evidence": ["22 passed, 22 warnings in 3.10s."]},
  "unresolved_gaps": [],
  "confidence": 0.99,
  "final_marker": null
}
"""
    router = _FakeLabRouter(tmp_path / "lab", status="running", transcript=transcript)
    router.sessions["lab-session-template-then-real"] = AOSession(
        id="lab-session-template-then-real",
        project_id="HermesAgentLab",
        status="running",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "template-then-real"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=2.0,
    )

    assert terminal["authoritative_terminal"] is True
    assert terminal["task"]["files_changed"] == ["docs/lab-dogfood-supervised.md"]


def test_lab_await_rejects_prompt_example_evidence_block(monkeypatch, tmp_path):
    db_path, _stable_db = _env(monkeypatch, tmp_path)
    execution_store = DevExecutionStore(db_path)
    event_store = SubagentEventStore(db_path)
    plan = execution_store.create_plan(
        title="Prompt example rejection",
        vision_brief="Do not accept the worker contract example.",
        tasks=[{
            "goal": "Append a docs note.",
            "prompt": "Append a docs note.",
            "profile_id": "platform.implement",
            "project_id": "HermesAgentLab",
            "target_paths": ["docs/lab-dogfood-supervised.md"],
        }],
    )
    task = plan["tasks"][0]
    execution_store.update_task_launch(plan_id=plan["plan_id"], task_id=task["task_id"], ao_session_id="lab-session-example")
    transcript = """Worker Output Contract v2
```json DEV_WORKER_EVIDENCE
{
  "summary": "What you concluded or changed.",
  "findings": ["Concrete finding or result."],
  "files_read": ["path/or/file.ext"],
  "files_changed": [],
  "commands_run": ["command --if-any"],
  "verification": {"status": "passed", "evidence": ["What proves the result."]},
  "unresolved_gaps": [],
  "confidence": 0.86,
  "final_marker": null
}
```
"""
    router = _FakeLabRouter(tmp_path / "lab", status="running", transcript=transcript)
    router.sessions["lab-session-example"] = AOSession(
        id="lab-session-example",
        project_id="HermesAgentLab",
        status="running",
        workspace_path=str(tmp_path / "lab" / "worktrees" / "example"),
    )

    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=router,
        timeout_seconds=0.01,
    )

    assert terminal["timed_out"] is True
    assert terminal["authoritative_terminal"] is False


def test_lab_diff_scope_ignores_bootstrap_venv(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Worker makes a docs change with a bootstrap symlink present.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
        "payload": {
            "verification_results": [{
                "criterion_id": "crit-1",
                "status": "passed",
                "command_run": "make test",
                "exit_code": 0,
                "output_excerpt": "1 passed in 0.1s",
            }],
        },
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["venv", "docs/lab.md"]),
        sources=["reliability"],
    )

    assert report["diff_scope"]["status"] == "in_scope"
    assert report["execution"]["touched_paths"] == ["docs/lab.md"]
    assert report["quarantined"] is False


def test_lab_executor_preserves_structured_acceptance_criteria(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    criterion = {
        "statement": "The lab observe-loop tests pass.",
        "verification_method": "test",
        "verification_detail": "scripts/run_tests.sh tests/gateway/test_lab_observe_loop.py -- -q",
        "machine_checkable": True,
    }
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Worker makes a docs change with an executable criterion.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
        "payload": {
            "acceptance_criteria": [criterion],
            "verification_results": [{
                "criterion_id": "crit-1",
                "status": "passed",
                "command_run": "scripts/run_tests.sh tests/gateway/test_lab_observe_loop.py -- -q",
                "exit_code": 0,
                "output_excerpt": "24 passed in 1.0s",
            }],
        },
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["docs/lab.md"]),
        sources=["reliability"],
    )

    task_criteria = report["execution"]["implement"]["plan"]["tasks"][0]["acceptance_criteria"]
    assert task_criteria == [criterion]
    assert isinstance(task_criteria[0], dict)
    assert report["execution"]["pre_verification_cleanup"]["cleaned"] is True


def test_lab_executor_quarantines_out_of_scope_worker_diff(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Worker attempts an engine edit despite scoped intent.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/lab_loop.py"],
        "source": "todo",
    }, approved=True)
    router = _FakeLabRouter(tmp_path / "lab", diff_paths=["agent/conversation_loop.py"])

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=router,
        sources=["reliability"],
        max_consecutive_failures=10,
    )

    assert report["status"] == "failed"
    assert report["diff_scope"]["status"] == "out_of_scope"
    assert "agent/conversation_loop.py" in report["diff_scope"]["rejected_paths"]
    assert report["draft_artifact"] is None
    outcome = DevReliabilityStore(db_path).list_outcomes(limit=1)[0]
    assert outcome["source_refs"]["quarantined"] is True
    assert outcome["source_refs"]["draft_pr_ready"] is False


def test_lab_adversarial_fixture_proves_post_diff_quarantine(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Compliant worker makes a docs change; fixture simulates a forbidden engine diff.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
        "payload": {
            "adversarial_diff_paths": ["agent/conversation_loop.py"],
        },
    }, approved=True)
    router = _FakeLabRouter(tmp_path / "lab", diff_paths=["docs/lab.md"])

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=router,
        sources=["reliability"],
        enable_adversarial_fixture=True,
        max_consecutive_failures=10,
    )

    assert report["status"] == "failed"
    assert report["quarantined"] is True
    assert report["diff_scope"]["status"] == "out_of_scope"
    assert report["diff_scope"]["rejected_paths"] == ["agent/conversation_loop.py"]
    assert report["draft_artifact"] is None
    fixture = report["execution"]["adversarial_fixture"]
    assert fixture["applied"] is True
    assert fixture["paths"] == ["agent/conversation_loop.py"]
    outcome = DevReliabilityStore(db_path).list_outcomes(limit=1)[0]
    assert outcome["source_refs"]["quarantined"] is True
    assert outcome["source_refs"]["adversarial_fixture"]["applied"] is True


def test_lab_adversarial_fixture_requires_explicit_enable(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Fixture request should be inert unless explicitly enabled.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/lab.md"],
        "source": "docs",
        "payload": {
            "verification_results": [{
                "criterion_id": "crit-1",
                "status": "passed",
                "command_run": "make test",
                "exit_code": 0,
                "output_excerpt": "1 passed in 0.1s",
            }],
            "adversarial_diff_paths": ["agent/conversation_loop.py"],
        },
    }, approved=True)
    router = _FakeLabRouter(tmp_path / "lab", diff_paths=["docs/lab.md"])

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=router,
        sources=["reliability"],
    )

    assert report["status"] == "completed"
    assert report["quarantined"] is False
    assert report["diff_scope"]["status"] == "in_scope"
    fixture = report["execution"]["adversarial_fixture"]
    assert fixture["requested"] is True
    assert fixture["enabled"] is False
    assert fixture["applied"] is False


def test_touched_paths_preserve_porcelain_paths_with_leading_status_space(tmp_path):
    repo = tmp_path / "repo"
    _init_git_repo(repo)
    target = repo / "agent" / "conversation_loop.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("print('seed')\n", encoding="utf-8")
    subprocess.run(["git", "add", "agent/conversation_loop.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "seed agent file"], cwd=repo, check=True)

    target.write_text("print('changed')\n", encoding="utf-8")
    paths = _touched_paths_from_worktree(repo)

    assert "agent/conversation_loop.py" in paths
    assert "gent/conversation_loop.py" not in paths


def test_lab_executor_records_empty_diff_as_failure(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Worker makes no changes.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/noop.md"],
        "source": "docs",
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=[]),
        sources=["reliability"],
        max_consecutive_failures=10,
    )

    assert report["status"] == "failed"
    assert report["empty_diff"] is True
    assert report["draft_artifact"] is None
    outcome = DevReliabilityStore(db_path).list_outcomes(limit=1)[0]
    assert outcome["source_refs"]["empty_diff"] is True


def test_lab_executor_worker_timeout_is_failed_outcome(monkeypatch, tmp_path):
    db_path, stable_db = _env(monkeypatch, tmp_path)
    monkeypatch.setenv("HERMES_DEV_LAB_WORKER_TIMEOUT_SECONDS", "0.01")
    DevLabLoopStore(db_path).upsert_candidate({
        "prompt": "Worker never reaches terminal state.",
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["docs/timeout.md"],
        "source": "docs",
    }, approved=True)

    report = run_lab_loop_pass(
        db_path=db_path,
        stable_db_path=stable_db,
        bridge=_FakeLabRouter(tmp_path / "lab", diff_paths=["docs/timeout.md"], status="running"),
        sources=["reliability"],
        max_consecutive_failures=10,
    )

    assert report["status"] == "failed"
    assert report["execution"]["implement"]["timed_out"] is True
    outcome = DevReliabilityStore(db_path).list_outcomes(limit=1)[0]
    assert outcome["terminal_status"] == "failed"
