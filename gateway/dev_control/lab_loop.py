"""Hermes Lab observe-loop runner and durable pass health."""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

from gateway.dev_control.acceptance_verification import (
    DevVerificationStore,
    launch_verification_run,
    refresh_verification_run,
)
from gateway.dev_control.ci_status import fetch_ci_status
from gateway.dev_control.dogfood_backlog import (
    dogfood_scope_check,
    is_guardrail_touching,
    normalize_candidate,
    normalize_target_path,
    preapproval_allows,
)
from gateway.dev_control.lab_environment import lab_paths_from_env, validate_lab_or_raise
from gateway.dev_control.lab_process_isolation import audit_current_process_isolation
from gateway.dev_control.production_signals import DevProductionSignalStore, run_signal_digest_sources
from gateway.dev_control.product_events import DevProductEventStore
from gateway.dev_control.reliability import DevReliabilityStore, outcome_excluded, scorecard
from gateway.dev_control.scm_lifecycle import build_code_review_prompt, parse_code_review_result
from gateway.dev_control.worker_output_contract import parse_worker_output_contract, worker_output_contract_score
from gateway.dev_execution import (
    DevExecutionStore,
    TASK_COMPLETED_STATUSES,
    TASK_FAILED_STATUSES,
    launch_execution_plan,
)
from gateway.subagent_events import SubagentEventStore
from hermes_state import apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_lab_dogfood_tasks (
    candidate_id TEXT PRIMARY KEY,
    prompt TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    target_paths TEXT NOT NULL,
    source TEXT NOT NULL,
    approved INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    scope_status TEXT NOT NULL,
    scope_warnings TEXT NOT NULL,
    guardrail_touching INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_lab_dogfood_status
    ON dev_lab_dogfood_tasks(status, approved, updated_at);

CREATE TABLE IF NOT EXISTS dev_lab_loop_passes (
    pass_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    candidate_id TEXT,
    outcome_id TEXT,
    report TEXT NOT NULL,
    breaker_reason TEXT,
    started_at REAL NOT NULL,
    completed_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_lab_loop_passes_completed
    ON dev_lab_loop_passes(completed_at DESC);

CREATE TABLE IF NOT EXISTS dev_lab_loop_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    status TEXT NOT NULL,
    halted_reason TEXT,
    consecutive_failure_count INTEGER NOT NULL DEFAULT 0,
    consecutive_out_of_scope_count INTEGER NOT NULL DEFAULT 0,
    last_pass_id TEXT,
    updated_at REAL NOT NULL,
    payload TEXT NOT NULL
);
"""

DIFF_SCOPE_IGNORED_PATHS = {
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
    "venv",
}


class DevLabLoopStore:
    """Durable dogfood backlog, pass reports, and breaker state."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        apply_wal_with_fallback(self._conn, db_label="state.db")
        self._lock = threading.Lock()
        with self._conn:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.execute(
                """
                INSERT OR IGNORE INTO dev_lab_loop_state (
                    id, status, halted_reason, consecutive_failure_count,
                    consecutive_out_of_scope_count, last_pass_id, updated_at, payload
                ) VALUES (1, 'idle', NULL, 0, 0, NULL, ?, '{}')
                """,
                (time.time(),),
            )

    def close(self) -> None:
        self._conn.close()

    def upsert_candidate(self, candidate: dict[str, Any], *, approved: Optional[bool] = None) -> dict[str, Any]:
        normalized = normalize_candidate(candidate, approved=approved)
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_lab_dogfood_tasks (
                    candidate_id, prompt, profile_id, risk_level, target_paths,
                    source, approved, status, scope_status, scope_warnings,
                    guardrail_touching, created_at, updated_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(candidate_id) DO UPDATE SET
                    prompt = excluded.prompt,
                    profile_id = excluded.profile_id,
                    risk_level = excluded.risk_level,
                    target_paths = excluded.target_paths,
                    source = excluded.source,
                    approved = excluded.approved,
                    scope_status = excluded.scope_status,
                    scope_warnings = excluded.scope_warnings,
                    guardrail_touching = excluded.guardrail_touching,
                    updated_at = excluded.updated_at,
                    payload = excluded.payload
                """,
                _candidate_values(normalized),
            )
        return self.get_candidate(normalized["candidate_id"]) or normalized

    def approve_candidate(self, candidate_id: str) -> dict[str, Any]:
        return self.update_candidate(candidate_id, {"approved": True, "status": "approved"})

    def update_candidate(self, candidate_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        current = self.get_candidate(candidate_id)
        if not current:
            raise KeyError(f"Dogfood candidate not found: {candidate_id}")
        merged = normalize_candidate({**current, **updates, "updated_at": time.time()})
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_lab_dogfood_tasks
                SET prompt = ?, profile_id = ?, risk_level = ?, target_paths = ?,
                    source = ?, approved = ?, status = ?, scope_status = ?,
                    scope_warnings = ?, guardrail_touching = ?, created_at = ?,
                    updated_at = ?, payload = ?
                WHERE candidate_id = ?
                """,
                (*_candidate_values(merged)[1:], candidate_id),
            )
        return self.get_candidate(candidate_id) or merged

    def get_candidate(self, candidate_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_lab_dogfood_tasks WHERE candidate_id = ?",
            (str(candidate_id or "").strip(),),
        ).fetchone()
        return _candidate_from_row(row) if row else None

    def list_candidates(self, *, status: Optional[str] = None, approved: Optional[bool] = None, limit: int = 50) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if approved is not None:
            clauses.append("approved = ?")
            params.append(1 if approved else 0)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_lab_dogfood_tasks
            {where}
            ORDER BY updated_at ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_candidate_from_row(row) for row in rows]

    def next_approved_candidate(self) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM dev_lab_dogfood_tasks
            WHERE approved = 1 AND status IN ('approved', 'candidate')
            ORDER BY updated_at ASC
            LIMIT 1
            """
        ).fetchone()
        return _candidate_from_row(row) if row else None

    def record_pass(self, report: dict[str, Any]) -> dict[str, Any]:
        payload = {
            **report,
            "pass_id": str(report.get("pass_id") or f"devlab-pass-{uuid.uuid4().hex[:10]}"),
            "status": str(report.get("status") or "completed").lower(),
            "started_at": float(report.get("started_at") or time.time()),
            "completed_at": float(report.get("completed_at") or time.time()),
        }
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_lab_loop_passes (
                    pass_id, status, candidate_id, outcome_id, report,
                    breaker_reason, started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["pass_id"],
                    payload["status"],
                    payload.get("candidate_id"),
                    payload.get("outcome_id"),
                    _json(payload),
                    payload.get("breaker_reason"),
                    payload["started_at"],
                    payload["completed_at"],
                ),
            )
        self.update_state_from_pass(payload)
        return payload

    def list_passes(self, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM dev_lab_loop_passes
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 20), 200)),),
        ).fetchall()
        return [_pass_from_row(row) for row in rows]

    def get_state(self) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM dev_lab_loop_state WHERE id = 1").fetchone()
        if not row:
            return {"status": "idle", "halted_reason": None, "consecutive_failure_count": 0}
        return _state_from_row(row)

    def resume(self) -> dict[str, Any]:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_lab_loop_state
                SET status = 'idle', halted_reason = NULL, consecutive_failure_count = 0,
                    consecutive_out_of_scope_count = 0, updated_at = ?, payload = '{}'
                WHERE id = 1
                """,
                (time.time(),),
            )
        return self.get_state()

    def halt(self, reason: str, *, pass_id: Optional[str] = None) -> dict[str, Any]:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_lab_loop_state
                SET status = 'halted', halted_reason = ?, last_pass_id = COALESCE(?, last_pass_id),
                    updated_at = ?, payload = ?
                WHERE id = 1
                """,
                (reason, pass_id, time.time(), _json({"reason": reason})),
            )
        return self.get_state()

    def update_state_from_pass(self, report: dict[str, Any]) -> dict[str, Any]:
        state = self.get_state()
        status = str(report.get("status") or "")
        failures = int(state.get("consecutive_failure_count") or 0)
        skips = int(state.get("consecutive_out_of_scope_count") or 0)
        if status in {"failed", "needs_attention"}:
            failures += 1
        elif status == "skipped" and report.get("skip_reason") == "out_of_scope":
            skips += 1
        else:
            failures = 0
            skips = 0
        next_status = "halted" if report.get("breaker_reason") else "idle"
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_lab_loop_state
                SET status = ?, halted_reason = ?, consecutive_failure_count = ?,
                    consecutive_out_of_scope_count = ?, last_pass_id = ?, updated_at = ?, payload = ?
                WHERE id = 1
                """,
                (
                    next_status,
                    report.get("breaker_reason"),
                    failures,
                    skips,
                    report.get("pass_id"),
                    time.time(),
                    _json({"last_report_status": status}),
                ),
            )
        return self.get_state()


def run_lab_loop_pass(
    *,
    db_path: Path,
    stable_db_path: Optional[Path] = None,
    bridge: Any = None,
    executor: Optional[Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]] = None,
    sources: Optional[list[str]] = None,
    max_consecutive_failures: int = 2,
    max_consecutive_out_of_scope: int = 3,
    max_seconds: float = 1800.0,
    max_cost_usd: Optional[float] = None,
    regression_threshold: float = 0.20,
    isolation_pids: Optional[list[int | str]] = None,
    enable_adversarial_fixture: Optional[bool] = None,
    now: Optional[float] = None,
) -> dict[str, Any]:
    started = float(now or time.time())
    paths = lab_paths_from_env()
    validate_lab_or_raise(
        hermes_home=Path(db_path).expanduser().parent,
        gateway_port=os.getenv("API_SERVER_PORT") or 8662,
        repo_roots=[Path(paths["repos_dir"]) / "hermes-agent", Path(paths["repos_dir"]) / "Oryn"],
    )
    stable_before = _mtime(stable_db_path)
    store = DevLabLoopStore(db_path)
    state = store.get_state()
    if state.get("status") == "halted":
        return {
            "ok": False,
            "object": "hermes.dev_lab_loop_pass",
            "status": "loop_halted",
            "halted_reason": state.get("halted_reason"),
            "started_at": started,
            "completed_at": time.time(),
        }
    candidate = store.next_approved_candidate()
    if not candidate:
        report = _base_report(started, "idle", candidate=None)
        report["message"] = "No approved dogfood candidate is queued."
        report["digest"] = _run_digest(db_path, sources=sources)
        _attach_isolation_fields(report, stable_before=stable_before, stable_db_path=stable_db_path, isolation_pids=isolation_pids)
        return store.record_pass(report)

    scope = dogfood_scope_check(candidate.get("target_paths") or [])
    if not scope["ok"]:
        store.update_candidate(candidate["candidate_id"], {"status": "skipped", "payload": {**candidate.get("payload", {}), "skip_reason": "out_of_scope"}})
        report = _base_report(started, "skipped", candidate=candidate)
        report["skip_reason"] = "out_of_scope"
        report["scope"] = scope
        _attach_isolation_fields(report, stable_before=stable_before, stable_db_path=stable_db_path, isolation_pids=isolation_pids)
        _apply_breakers(report, store, max_consecutive_failures=max_consecutive_failures, max_consecutive_out_of_scope=max_consecutive_out_of_scope)
        return store.record_pass(report)

    if is_guardrail_touching(candidate) and not bool(candidate.get("approved")):
        store.update_candidate(candidate["candidate_id"], {"status": "skipped", "payload": {**candidate.get("payload", {}), "skip_reason": "guardrail_requires_approval"}})
        report = _base_report(started, "skipped", candidate=candidate)
        report["skip_reason"] = "guardrail_requires_approval"
        return store.record_pass(report)

    before_scorecard = scorecard(DevReliabilityStore(db_path).list_outcomes(limit=5000))
    run_executor = executor or local_observe_executor
    try:
        execution = run_executor(candidate, {
            "db_path": db_path,
            "started_at": started,
            "bridge": bridge,
            "max_seconds": max_seconds,
            "enable_adversarial_fixture": (
                bool(enable_adversarial_fixture)
                if enable_adversarial_fixture is not None
                else _env_bool("HERMES_DEV_LAB_ENABLE_ADVERSARIAL_FIXTURE", False)
            ),
        })
    except Exception as exc:  # noqa: BLE001 - loop reports failed pass and lets breakers decide.
        execution = {"status": "failed", "error": str(exc)}
    digest = _run_digest(db_path, sources=sources)
    after_scorecard = scorecard(DevReliabilityStore(db_path).list_outcomes(limit=5000))
    status = "completed" if execution.get("status") == "completed" else "failed"
    store.update_candidate(candidate["candidate_id"], {"status": status})
    report = _base_report(started, status, candidate=candidate)
    report.update({
        "execution": execution,
        "outcome_id": execution.get("outcome_id"),
        "gate_verdicts": {
            "verification": execution.get("verification_verdict") or "unknown",
            "ci": execution.get("ci_state") or "unknown",
            "review": execution.get("code_review_verdict") or "unknown",
            "contract_score": execution.get("output_contract_score"),
        },
        "implement_session_id": execution.get("implement_session_id"),
        "branch": execution.get("branch"),
        "draft_artifact": execution.get("draft_artifact"),
        "diff_scope": execution.get("diff_scope"),
        "empty_diff": bool(execution.get("empty_diff")),
        "quarantined": bool(execution.get("quarantined")),
        "digest": digest,
        "scorecard_before": _scorecard_summary(before_scorecard),
        "scorecard_after": _scorecard_summary(after_scorecard),
    })
    _attach_isolation_fields(report, stable_before=stable_before, stable_db_path=stable_db_path, isolation_pids=isolation_pids)
    _apply_breakers(
        report,
        store,
        max_consecutive_failures=max_consecutive_failures,
        max_consecutive_out_of_scope=max_consecutive_out_of_scope,
        max_seconds=max_seconds,
        max_cost_usd=max_cost_usd,
        regression_threshold=regression_threshold,
    )
    return store.record_pass(report)


def run_lab_loop(
    *,
    db_path: Path,
    stable_db_path: Optional[Path] = None,
    max_passes: int = 1,
    sources: Optional[list[str]] = None,
    **kwargs: Any,
) -> dict[str, Any]:
    passes = []
    ok = True
    for _ in range(max(1, int(max_passes or 1))):
        report = run_lab_loop_pass(db_path=db_path, stable_db_path=stable_db_path, sources=sources, **kwargs)
        passes.append(report)
        if report.get("status") in {"loop_halted"} or report.get("breaker_reason"):
            ok = False
            break
    return {
        "ok": ok,
        "object": "hermes.dev_lab_loop_run",
        "passes": passes,
        "pass_count": len(passes),
        "state": DevLabLoopStore(db_path).get_state(),
    }


def local_observe_executor(candidate: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """R2 lab executor: dispatches implementation work, then records measured/unknown gates."""

    db_path = Path(context["db_path"])
    bridge = context.get("bridge")
    execution_store = DevExecutionStore(db_path)
    reliability_store = DevReliabilityStore(db_path)
    event_store = SubagentEventStore(db_path)
    now = time.time()
    acceptance_criteria = _candidate_acceptance_criteria(candidate)
    task_branch = _task_branch(candidate)
    project_id = _lab_project_id(candidate)
    prompt = _lab_worker_prompt(candidate, acceptance_criteria)
    plan = execution_store.create_plan(
        title=f"Dogfood: {candidate.get('source')} improvement",
        vision_brief=candidate.get("prompt") or "Hermes Lab dogfood task.",
        tasks=[{
            "goal": str(candidate.get("prompt") or "Dogfood harness task")[:180],
            "prompt": prompt,
            "profile_id": candidate.get("profile_id"),
            "project_id": project_id,
            "risk_level": candidate.get("risk_level"),
            "acceptance_criteria": acceptance_criteria,
            "target_paths": candidate.get("target_paths") or [],
            "branch": task_branch,
            "payload": {
                "branch": task_branch,
                "lab_loop": True,
                "candidate_id": candidate.get("candidate_id"),
                "target_paths": candidate.get("target_paths") or [],
            },
        }],
    )
    plan = _preserve_lab_structured_acceptance_criteria(
        execution_store=execution_store,
        plan=plan,
        acceptance_criteria=acceptance_criteria,
    )
    task = (plan.get("tasks") or [])[0]
    implementation = _implement_via_worker(
        candidate=candidate,
        db_path=db_path,
        execution_store=execution_store,
        event_store=event_store,
        plan=plan,
        task=task,
        bridge=bridge,
        timeout_seconds=_worker_timeout_seconds(context),
    )
    derived_plan = implementation.get("plan") or execution_store.get_plan(plan["plan_id"]) or plan
    derived_task = next(
        (item for item in derived_plan.get("tasks") or [] if item.get("task_id") == task.get("task_id")),
        task,
    )
    workspace_path = implementation.get("workspace_path") or derived_task.get("workspace_path")
    adversarial_fixture = _apply_adversarial_diff_fixture(
        candidate=candidate,
        workspace_path=workspace_path,
        enabled=bool(context.get("enable_adversarial_fixture")),
    )
    if adversarial_fixture.get("requested"):
        implementation["adversarial_fixture"] = adversarial_fixture
    touched_paths = _touched_paths_from_worktree(
        workspace_path,
        fallback_paths=(derived_task.get("files_written") or derived_task.get("files_changed") or []),
        base_ref=(candidate.get("payload") or {}).get("diff_base_ref") if isinstance(candidate.get("payload"), dict) else None,
    )
    diff_scope = _diff_scope_result(touched_paths)
    empty_diff = not touched_paths
    quarantined = not diff_scope.get("ok")
    draft_artifact = None
    if implementation.get("status") == "completed" and not quarantined and not empty_diff:
        draft_artifact = _draft_branch_artifact(
            candidate=candidate,
            implementation=implementation,
            workspace_path=workspace_path,
        )
    pre_verification_cleanup = None
    if draft_artifact:
        pre_verification_cleanup = _cleanup_lab_worktree(workspace_path)
        implementation["pre_verification_cleanup"] = pre_verification_cleanup

    verification = _measure_verification_r1(
        candidate=candidate,
        db_path=db_path,
        execution_store=execution_store,
        event_store=event_store,
        bridge=bridge,
        plan=derived_plan,
        task=derived_task,
        acceptance_criteria=acceptance_criteria,
        implementation=implementation,
        diff_scope=diff_scope,
        draft_artifact=draft_artifact,
    )
    verification_verdict = verification.get("verdict") or "unknown"
    ci_status = _measure_ci_r3(
        candidate=candidate,
        draft_artifact=draft_artifact,
        head_sha=implementation.get("head_sha"),
        fetcher=context.get("ci_status_fetcher"),
    )
    ci_state = str(ci_status.get("state") or verification.get("ci_state") or "unknown")
    code_review = _measure_code_review_r4(
        candidate=candidate,
        context=context,
        plan=derived_plan,
        task=derived_task,
        draft_artifact=draft_artifact,
        implementation=implementation,
        verification=verification,
    )
    code_review_verdict = str(code_review.get("verdict") or verification.get("code_review_verdict") or "unknown")
    output_contract_score = code_review.get("output_contract_score")
    if output_contract_score is None:
        output_contract_score = verification.get("output_contract_score")
    draft_pr_ready = bool(draft_artifact and draft_artifact.get("ready"))
    terminal_status = _lab_terminal_status(
        implementation=implementation,
        verification_verdict=verification_verdict,
        quarantined=quarantined,
        empty_diff=empty_diff,
    )
    exclude_from_scorecard = _execution_excluded_from_scorecard(implementation)
    outcome = None
    if not exclude_from_scorecard:
        outcome = reliability_store.upsert_outcome({
            "plan_id": plan["plan_id"],
            "task_id": task["task_id"],
            "profile_id": candidate.get("profile_id") or "platform.implement",
            "risk_level": candidate.get("risk_level") or "low",
            "terminal_status": terminal_status,
            "merged": False,
            "verification_verdict": verification_verdict,
            "ci_state": ci_state,
            "code_review_verdict": code_review_verdict,
            "output_contract_score": output_contract_score,
            "rework_count": 0,
            "escaped": False,
            "source_refs": {
                "source": "dogfood_lab_loop",
                "seeded": False,
                "candidate_id": candidate.get("candidate_id"),
                "target_paths": candidate.get("target_paths") or [],
                "draft_pr_only": True,
                "draft_pr_ready": draft_pr_ready,
                "branch": implementation.get("branch"),
                "head_sha": implementation.get("head_sha"),
                "implement_session_id": implementation.get("session_id"),
                "implement_status": implementation.get("status"),
                "workspace_path": workspace_path,
                "touched_paths": touched_paths,
                "diff_scope": diff_scope,
                "quarantined": quarantined,
                "empty_diff": empty_diff,
                "draft_artifact": draft_artifact,
                "ci_status": ci_status,
                "code_review": code_review,
                "adversarial_fixture": adversarial_fixture if adversarial_fixture.get("requested") else None,
                "gates": {
                    "verification": verification.get("status") or "unknown",
                    "ci": ci_status.get("status") or ("not_measured" if ci_state == "unknown" else ci_state),
                    "review": code_review.get("status") or ("not_measured" if code_review_verdict == "unknown" else code_review_verdict),
                },
                "verification_run_id": verification.get("verification_run_id"),
            },
            "created_at": now,
            "updated_at": now,
            "completed_at": now,
            "merged_at": None,
        })
    review_failed = bool(code_review.get("measured")) and code_review_verdict != "approved"
    failed = (
        terminal_status == "failed"
        or verification_verdict in {"failed", "partial", "needs_review"}
        or ci_state == "failure"
        or review_failed
    )
    result = {
        "status": "failed" if failed else "completed",
        "plan_id": plan["plan_id"],
        "task_id": task["task_id"],
        "outcome_id": (outcome or {}).get("outcome_id"),
        "scorecard_excluded": exclude_from_scorecard,
        "implement": implementation,
        "implement_session_id": implementation.get("session_id"),
        "implementation_status": implementation.get("status"),
        "implementation_reason": implementation.get("reason"),
        "workspace_path": workspace_path,
        "branch": implementation.get("branch"),
        "head_sha": implementation.get("head_sha"),
        "touched_paths": touched_paths,
        "diff_scope": diff_scope,
        "empty_diff": empty_diff,
        "quarantined": quarantined,
        "draft_artifact": draft_artifact,
        "verification": verification,
        "verification_verdict": verification_verdict,
        "ci_status": ci_status,
        "ci_state": ci_state,
        "code_review": code_review,
        "code_review_verdict": code_review_verdict,
        "output_contract_score": output_contract_score,
        "draft_pr_only": True,
        "draft_pr_ready": draft_pr_ready,
        "merge_executed": False,
        "publish_executed": False,
        "cost_usd": implementation.get("cost_usd"),
        "duration_seconds": implementation.get("duration_seconds"),
        "pre_verification_cleanup": pre_verification_cleanup,
        "adversarial_fixture": adversarial_fixture if adversarial_fixture.get("requested") else None,
    }
    _cleanup_lab_worktree(workspace_path)
    return result


def _implement_via_worker(
    *,
    candidate: dict[str, Any],
    db_path: Path,
    execution_store: DevExecutionStore,
    event_store: SubagentEventStore,
    plan: dict[str, Any],
    task: dict[str, Any],
    bridge: Any,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.time()
    try:
        launch = launch_execution_plan(
            store=execution_store,
            plan_id=plan["plan_id"],
            task_ids=[task["task_id"]],
            bridge=bridge,
            event_store=event_store,
        )
    except Exception as exc:  # noqa: BLE001 - launch failure is a measured lab failure.
        return {
            "status": "failed",
            "reason": f"worker_launch_failed:{exc}",
            "duration_seconds": round(time.time() - started, 3),
            "candidate_id": candidate.get("candidate_id"),
        }
    launched = (launch.get("launched") or [None])[0]
    if not launched:
        return {
            "status": "failed",
            "reason": f"worker_launch_failed:{launch.get('failures') or 'no session launched'}",
            "duration_seconds": round(time.time() - started, 3),
            "candidate_id": candidate.get("candidate_id"),
        }
    session_id = str(launched.get("ao_session_id") or launched.get("runtime_session_id") or "").strip()
    runtime = str(launched.get("runtime") or launched.get("selected_runtime") or "ao").strip() or "ao"
    terminal = _await_implementation_terminal(
        execution_store=execution_store,
        event_store=event_store,
        plan_id=plan["plan_id"],
        task_id=task["task_id"],
        bridge=bridge,
        timeout_seconds=timeout_seconds,
    )
    task_state = terminal.get("task") or task
    status = str(task_state.get("status") or terminal.get("status") or "unknown").lower()
    timed_out = bool(terminal.get("timed_out"))
    failed = timed_out or status in {"failed", "needs_review"}
    return {
        "status": "failed" if failed else "completed" if status == "completed" else status,
        "reason": terminal.get("reason") or task_state.get("status_reason"),
        "timed_out": timed_out,
        "session_id": session_id,
        "runtime": runtime,
        "project_id": task_state.get("runtime_project_id") or task_state.get("project_id") or task.get("project_id"),
        "workspace_path": task_state.get("workspace_path"),
        "branch": task_state.get("branch") or (task.get("payload") or {}).get("branch"),
        "head_sha": _git_head_sha(task_state.get("workspace_path")),
        "files_written": task_state.get("files_written") or [],
        "files_changed": task_state.get("files_changed") or task_state.get("files_written") or [],
        "output_contract_score": task_state.get("output_contract_score"),
        "cost_usd": _first_numeric(task_state.get("cost_usd"), (task_state.get("last_event") or {}).get("cost_usd")),
        "duration_seconds": round(time.time() - started, 3),
        "plan": terminal.get("plan") or launch.get("plan"),
        "launch": launched,
        "candidate_id": candidate.get("candidate_id"),
    }


def _await_implementation_terminal(
    *,
    execution_store: DevExecutionStore,
    event_store: SubagentEventStore,
    plan_id: str,
    task_id: str,
    bridge: Any,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(timeout_seconds or 1.0))
    started = time.monotonic()
    min_terminal_seconds = _env_float("HERMES_DEV_LAB_MIN_TERMINAL_SECONDS", 5.0)
    last_payload: dict[str, Any] = {}
    while True:
        plan = execution_store.get_plan(plan_id) or {"plan_id": plan_id, "tasks": []}
        task = next((item for item in plan.get("tasks") or [] if item.get("task_id") == task_id), None)
        authoritative = _authoritative_lab_terminal(
            task=task or {},
            bridge=bridge,
            event_store=event_store,
            elapsed_seconds=time.monotonic() - started,
            min_terminal_seconds=min_terminal_seconds,
        )
        terminal_task = {**(task or {}), **(authoritative.get("task_updates") or {})}
        last_payload = {
            "plan": plan,
            "task": terminal_task or task,
            "status": authoritative.get("status") or (task or {}).get("status"),
            "reason": authoritative.get("reason"),
            "authoritative_terminal": authoritative.get("terminal"),
        }
        if authoritative.get("terminal"):
            return last_payload
        if time.monotonic() >= deadline:
            return {**last_payload, "timed_out": True, "reason": f"worker_timeout:{timeout_seconds:.1f}s"}
        time.sleep(1.0)


def _authoritative_lab_terminal(
    *,
    task: dict[str, Any],
    bridge: Any,
    event_store: SubagentEventStore,
    elapsed_seconds: float,
    min_terminal_seconds: float,
) -> dict[str, Any]:
    session_id = str(task.get("ao_session_id") or task.get("runtime_session_id") or "").strip()
    if not session_id:
        return {"terminal": False, "status": "planned", "reason": "Worker session has not launched."}
    runtime = str(task.get("runtime") or task.get("selected_runtime") or "ao").strip() or "ao"
    session = _lab_runtime_session(
        bridge,
        runtime,
        session_id,
        project_id=task.get("project_id") or task.get("runtime_project_id"),
    )
    session_status = _session_status(session)
    session_updates = _session_task_updates(session)
    if session_status in TASK_COMPLETED_STATUSES:
        if elapsed_seconds < max(0.0, float(min_terminal_seconds or 0.0)):
            return {
                "terminal": False,
                "status": "running",
                "reason": "Ignoring implausibly early completed worker session.",
                "task_updates": session_updates,
            }
        return {
            "terminal": True,
            "status": "completed",
            "reason": "Worker session reached terminal completed state.",
            "task_updates": {**session_updates, "status": "completed", "derived_status": "completed"},
        }
    if session_status in TASK_FAILED_STATUSES:
        return {
            "terminal": True,
            "status": "failed",
            "reason": f"Worker session ended with status {session_status}.",
            "task_updates": {**session_updates, "status": "failed", "derived_status": "failed"},
        }

    event = _authoritative_complete_event(event_store, session_id, plan_id=task.get("plan_id"), task_id=task.get("task_id"))
    if event:
        event_status = str(event.get("status") or "completed").lower()
        status = "failed" if event_status in TASK_FAILED_STATUSES else "completed"
        if status == "completed" and elapsed_seconds < max(0.0, float(min_terminal_seconds or 0.0)):
            return {
                "terminal": False,
                "status": "running",
                "reason": "Ignoring implausibly early subagent.complete event.",
                "task_updates": session_updates,
            }
        return {
            "terminal": True,
            "status": status,
            "reason": "Worker emitted authoritative subagent.complete event.",
            "task_updates": {**session_updates, "status": status, "derived_status": status, "last_event": event},
        }

    transcript_event = _authoritative_transcript_completion(
        task=task,
        bridge=bridge,
        runtime=runtime,
        session=session,
        session_id=session_id,
        event_store=event_store,
    )
    if transcript_event:
        if elapsed_seconds < max(0.0, float(min_terminal_seconds or 0.0)):
            return {
                "terminal": False,
                "status": "running",
                "reason": "Ignoring implausibly early worker evidence block.",
                "task_updates": session_updates,
            }
        return {
            "terminal": True,
            "status": "completed",
            "reason": "Worker transcript included valid DEV_WORKER_EVIDENCE.",
            "task_updates": {
                **session_updates,
                **output_contract_fields_from_lab_event(transcript_event),
                "status": "completed",
                "derived_status": "completed",
                "last_event": transcript_event,
            },
        }

    return {
        "terminal": False,
        "status": "running",
        "reason": "Worker session is still running.",
        "task_updates": session_updates,
    }


def _lab_runtime_session(bridge: Any, runtime: str, session_id: str, *, project_id: Any = None) -> Any:
    if bridge is None or not session_id:
        return None
    try:
        return bridge.status(runtime, session_id)
    except TypeError:
        try:
            return bridge.status(session_id)
        except Exception:
            pass
    except Exception:
        pass
    try:
        for session in bridge.list(runtime, project_id=project_id):
            if str(getattr(session, "id", "") or "") == session_id:
                return session
    except Exception:
        pass
    return None


def _session_status(session: Any) -> str:
    for name in ("display_status", "status"):
        value = _session_value(session, name)
        if str(value or "").strip():
            return str(value).strip().lower()
    return ""


def _session_task_updates(session: Any) -> dict[str, Any]:
    if session is None:
        return {}
    return {
        key: value
        for key, value in {
            "runtime_project_id": _session_value(session, "project_id"),
            "ao_project_id": _session_value(session, "project_id"),
            "workspace_path": _session_value(session, "workspace_path"),
            "branch": _session_value(session, "branch"),
            "agent": _session_value(session, "agent"),
            "model": _session_value(session, "model"),
            "reasoning_effort": _session_value(session, "reasoning_effort"),
            "tmux_name": _session_value(session, "tmux_name"),
        }.items()
        if value is not None and value != ""
    }


def _session_value(session: Any, name: str) -> Any:
    if isinstance(session, dict):
        return session.get(name)
    return getattr(session, name, None)


def _authoritative_complete_event(
    event_store: SubagentEventStore,
    session_id: str,
    *,
    plan_id: Any,
    task_id: Any,
) -> Optional[dict[str, Any]]:
    if not event_store or not session_id:
        return None
    try:
        events = event_store.list_events(ao_session_id=session_id, limit=200)
    except Exception:
        return None
    plan_text = str(plan_id or "").strip()
    task_text = str(task_id or "").strip()
    for event in reversed(events):
        if event.get("transcript_inferred_completion") or event.get("transcript_corrected"):
            continue
        if str(event.get("event") or "").lower() != "subagent.complete":
            continue
        if plan_text and str(event.get("launch_plan_id") or "").strip() not in {"", plan_text}:
            continue
        if task_text and str(event.get("launch_task_id") or "").strip() not in {"", task_text}:
            continue
        return event
    return None


def _authoritative_transcript_completion(
    *,
    task: dict[str, Any],
    bridge: Any,
    runtime: str,
    session: Any,
    session_id: str,
    event_store: SubagentEventStore,
) -> Optional[dict[str, Any]]:
    if bridge is None or session is None:
        return None
    try:
        transcript = bridge.capture_output(runtime, session, lines=240) or ""
    except Exception:
        return None
    fields = _parse_lab_worker_evidence(transcript)
    if not _lab_worker_evidence_is_terminal(fields, task=task):
        return None
    marker = fields.get("final_marker")
    fields["output_contract_score"] = worker_output_contract_score(fields, required_marker=marker)
    payload = {
        "event": "subagent.complete",
        "subagent_id": f"{runtime}:{session_id}",
        "ao_session_id": session_id if runtime == "ao" else None,
        "runtime": runtime,
        "runtime_session_id": session_id,
        "runtime_project_id": task.get("project_id") or task.get("runtime_project_id"),
        "launch_plan_id": task.get("plan_id"),
        "launch_task_id": task.get("task_id"),
        "status": "completed",
        "summary": fields.get("structured_summary"),
        "message": fields.get("structured_summary"),
        "preview": fields.get("structured_summary"),
        "goal": task.get("goal"),
        "transcript_evidence_completion": True,
        "tool": "dev_lab_loop",
        "tool_name": "dev_lab_loop",
        **fields,
    }
    try:
        return event_store.append_event(payload)
    except Exception:
        payload["created_at"] = time.time()
        return payload


def _lab_worker_evidence_is_terminal(fields: dict[str, Any], *, task: dict[str, Any]) -> bool:
    status = str(fields.get("output_contract_status") or "").lower()
    if status in {"", "missing", "invalid"}:
        return False
    summary = str(fields.get("structured_summary") or "").strip()
    if not summary or summary == "What you concluded or changed.":
        return False
    changed = [str(path or "").strip() for path in fields.get("files_changed") or [] if str(path or "").strip()]
    if not changed:
        return False
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    target_paths = [
        str(path or "").strip()
        for path in (task.get("target_paths") or payload.get("target_paths") or [])
        if str(path or "").strip()
    ]
    if target_paths and not any(_path_matches_target(path, target_paths) for path in changed):
        return False
    verification_status = str(fields.get("verification_status") or "").lower()
    if verification_status not in {"passed", "failed", "partial"}:
        return False
    return True


def _parse_lab_worker_evidence(transcript: str) -> dict[str, Any]:
    fields = parse_worker_output_contract(transcript)
    if fields.get("output_contract_status") not in {"missing", "invalid"} and not _worker_evidence_is_prompt_template(fields):
        return fields
    loose = _parse_loose_lab_worker_evidence(transcript)
    return loose or fields


def _worker_evidence_is_prompt_template(fields: dict[str, Any]) -> bool:
    summary = str(fields.get("structured_summary") or "").strip()
    if summary == "What you concluded or changed.":
        return True
    evidence = [str(item or "").strip() for item in fields.get("verification_evidence") or []]
    if evidence == ["What proves the result."]:
        return True
    return False


def _parse_loose_lab_worker_evidence(transcript: str) -> Optional[dict[str, Any]]:
    text = str(transcript or "")
    lower = text.lower()
    if '"files_changed"' not in lower or '"verification"' not in lower:
        return None
    summary_pos = lower.rfind('"summary"')
    start = summary_pos if summary_pos >= 0 else lower.rfind("{\n")
    tail = text[start:] if start >= 0 else text
    summary = _last_jsonish_string(tail, "summary")
    files_read = _last_jsonish_string_array(tail, "files_read")
    files_changed = _last_jsonish_string_array(tail, "files_changed")
    commands_run = _last_jsonish_string_array(tail, "commands_run")
    findings = _last_jsonish_string_array(tail, "findings")
    unresolved_gaps = _last_jsonish_string_array(tail, "unresolved_gaps")
    verification_status = _last_jsonish_verification_status(tail)
    verification_evidence = _last_jsonish_verification_evidence(tail)
    if not summary or not files_changed or verification_status not in {"passed", "failed", "partial", "not_run"}:
        return None
    fields = {
        "output_contract_version": 2,
        "output_contract_status": "warning",
        "output_contract_warning": "Worker evidence was parsed from an unfenced transcript block.",
        "structured_summary": summary,
        "findings": findings,
        "files_read": files_read,
        "files_changed": files_changed,
        "commands_run": commands_run,
        "verification_status": verification_status,
        "verification_evidence": verification_evidence,
        "unresolved_gaps": unresolved_gaps,
        "worker_confidence": _last_jsonish_confidence(tail),
        "final_marker": None,
    }
    fields["output_contract_score"] = worker_output_contract_score(fields)
    return fields


def _last_jsonish_string(text: str, key: str) -> Optional[str]:
    matches = list(re.finditer(rf'"{re.escape(key)}"\s*:\s*"(?P<value>.*?)"', text, re.DOTALL))
    if not matches:
        return None
    value = matches[-1].group("value")
    return " ".join(value.replace('\\"', '"').split())


def _last_jsonish_string_array(text: str, key: str) -> list[str]:
    matches = list(re.finditer(rf'"{re.escape(key)}"\s*:\s*\[(?P<value>.*?)\]', text, re.DOTALL))
    if not matches:
        return []
    raw = matches[-1].group("value")
    return [" ".join(match.group(1).split()) for match in re.finditer(r'"(.*?)"', raw, re.DOTALL)]


def _last_jsonish_verification_status(text: str) -> Optional[str]:
    matches = list(re.finditer(r'"verification"\s*:\s*\{(?P<value>.*?)\}', text, re.DOTALL))
    if not matches:
        return None
    return (_last_jsonish_string(matches[-1].group("value"), "status") or "").lower() or None


def _last_jsonish_verification_evidence(text: str) -> list[str]:
    matches = list(re.finditer(r'"verification"\s*:\s*\{(?P<value>.*?)\}', text, re.DOTALL))
    if not matches:
        return []
    return _last_jsonish_string_array(matches[-1].group("value"), "evidence")


def _last_jsonish_confidence(text: str) -> Optional[float]:
    matches = list(re.finditer(r'"confidence"\s*:\s*([0-9.]+)', text))
    if not matches:
        return None
    try:
        return float(matches[-1].group(1))
    except ValueError:
        return None


def _path_matches_target(path: str, target_paths: list[str]) -> bool:
    normalized = path.strip().strip("/")
    return any(
        normalized == target.strip().strip("/")
        or normalized.startswith(f"{target.strip().strip('/')}/")
        for target in target_paths
    )


def output_contract_fields_from_lab_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "files_written": event.get("files_changed") or [],
        "files_changed": event.get("files_changed") or [],
        "files_read": event.get("files_read") or [],
        "commands_run": event.get("commands_run") or [],
        "output_contract_score": event.get("output_contract_score"),
        "worker_confidence": event.get("worker_confidence"),
    }


def _candidate_acceptance_criteria(candidate: dict[str, Any]) -> list[Any]:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    criteria = payload.get("acceptance_criteria")
    if isinstance(criteria, list) and criteria:
        return criteria
    return [{
        "statement": "The lab dogfood task has measurable verification evidence.",
        "verification_method": "manual",
        "verification_detail": "Review the pass report and linked task evidence.",
        "machine_checkable": False,
    }]


def _preserve_lab_structured_acceptance_criteria(
    *,
    execution_store: DevExecutionStore,
    plan: dict[str, Any],
    acceptance_criteria: list[Any],
) -> dict[str, Any]:
    """Keep lab-only structured criteria available for the verifier.

    DevExecutionStore normally stringifies task-level criteria for worker prompts.
    The lab loop needs the structured verification_detail fields later so the
    middle gate can execute machine-checkable criteria from the dogfood candidate.
    """

    if not acceptance_criteria:
        return plan
    task = (plan.get("tasks") or [None])[0]
    if not task:
        return plan
    task_id = task.get("task_id")
    plan_id = plan.get("plan_id")
    if not task_id or not plan_id:
        return plan
    with execution_store._lock, execution_store._conn:  # noqa: SLF001 - lab-only compatibility shim.
        execution_store._conn.execute(  # noqa: SLF001
            """
            UPDATE dev_execution_plan_tasks
            SET acceptance_criteria = ?, updated_at = ?
            WHERE plan_id = ? AND task_id = ?
            """,
            (json.dumps(acceptance_criteria, ensure_ascii=False), time.time(), plan_id, task_id),
        )
    return execution_store.get_plan(plan_id) or plan


def _measure_verification_r1(
    *,
    candidate: dict[str, Any],
    db_path: Path,
    execution_store: DevExecutionStore,
    event_store: SubagentEventStore,
    bridge: Any,
    plan: dict[str, Any],
    task: dict[str, Any],
    acceptance_criteria: list[Any],
    implementation: dict[str, Any],
    diff_scope: dict[str, Any],
    draft_artifact: Optional[dict[str, Any]],
) -> dict[str, Any]:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    fixture_results = payload.get("verification_results")
    if isinstance(fixture_results, list):
        run = DevVerificationStore(db_path).create_run(
            plan_id=plan["plan_id"],
            task_id=task["task_id"],
            target_type="task",
            status="completed",
            results=fixture_results,
            executable_commands=[],
            verified_against={
                "source": "lab_loop_fixture",
                "draft_pr_only": True,
                "verification_relative_cwd": ".",
                "workspace_path": implementation.get("workspace_path"),
                "branch": implementation.get("branch"),
                "head_sha": implementation.get("head_sha"),
                "diff_scope": diff_scope,
                "draft_artifact": draft_artifact,
            },
            warnings=[],
        )
        return {
            "status": run.get("status"),
            "verdict": run.get("verdict"),
            "verification_run_id": run.get("verification_run_id"),
            "counts": run.get("counts") or {},
            "score": run.get("acceptance_verification_score"),
            "measured": True,
        }
    if implementation.get("status") != "completed":
        return {
            "status": "not_measured",
            "verdict": "unknown",
            "verification_run_id": None,
            "counts": {},
            "score": None,
            "measured": False,
            "warnings": [f"Verification was not launched because implementation status is {implementation.get('status')}."],
        }
    if not diff_scope.get("ok"):
        return {
            "status": "quarantined",
            "verdict": "unknown",
            "verification_run_id": None,
            "counts": {},
            "score": None,
            "measured": False,
            "warnings": ["Verification was not launched because the implementation diff touched out-of-scope paths."],
        }
    if not draft_artifact:
        return {
            "status": "not_measured",
            "verdict": "unknown",
            "verification_run_id": None,
            "counts": {},
            "score": None,
            "measured": False,
            "warnings": [
                "Verification was not launched because the implementation produced no draft branch artifact.",
                f"acceptance_criteria_count={len(acceptance_criteria)}",
            ],
        }
    verification_store = DevVerificationStore(db_path)
    try:
        run = launch_verification_run(
            execution_store=execution_store,
            verification_store=verification_store,
            plan_id=plan["plan_id"],
            task_id=task.get("task_id"),
            bridge=bridge,
            event_store=event_store,
        )
    except ValueError as exc:
        return {
            "status": "not_measured",
            "verdict": "unknown",
            "verification_run_id": None,
            "counts": {},
            "score": None,
            "measured": False,
            "warnings": [str(exc)],
        }
    except Exception as exc:  # noqa: BLE001 - verification launch failures are surfaced, not hidden.
        return {
            "status": "needs_attention",
            "verdict": "unknown",
            "verification_run_id": None,
            "counts": {},
            "score": None,
            "measured": False,
            "warnings": [f"Verification launch failed: {exc}"],
        }
    if run.get("status") == "launched":
        deadline = time.monotonic() + _env_float("HERMES_DEV_LAB_VERIFY_TIMEOUT_SECONDS", 900.0)
        while time.monotonic() < deadline:
            refreshed = refresh_verification_run(
                verification_store=verification_store,
                verification_run_id=run["verification_run_id"],
                event_store=event_store,
                bridge=bridge,
            )
            run = refreshed
            if run.get("status") in {"completed", "skipped", "needs_attention"}:
                break
            time.sleep(1.0)
        if run.get("status") == "launched":
            run = verification_store.update_run(run["verification_run_id"], {
                "status": "needs_attention",
                "warnings": [*(run.get("warnings") or []), "Lab verification worker did not complete before timeout."],
            })
    return {
        "status": run.get("status"),
        "verdict": run.get("verdict") or "unknown",
        "verification_run_id": run.get("verification_run_id"),
        "counts": run.get("counts") or {},
        "score": run.get("acceptance_verification_score"),
        "measured": run.get("status") in {"completed", "skipped", "needs_attention"},
        "warnings": run.get("warnings") or [],
    }


def _measure_ci_r3(
    *,
    candidate: dict[str, Any],
    draft_artifact: Optional[dict[str, Any]],
    head_sha: Any,
    fetcher: Optional[Callable[..., dict[str, Any]]] = None,
) -> dict[str, Any]:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    fixture = payload.get("ci_status")
    if isinstance(fixture, dict):
        state = str(fixture.get("state") or "unknown").strip().lower()
        return {
            "status": state if state != "unknown" else "not_measured",
            "state": state,
            "measured": state != "unknown",
            "source": "fixture",
            "repo": fixture.get("repo"),
            "ref": fixture.get("ref") or head_sha,
            "raw": fixture,
            "warnings": fixture.get("warnings") or [],
        }
    repo = str(payload.get("ci_repo") or os.getenv("HERMES_DEV_LAB_CI_REPO") or "").strip()
    ref = str(payload.get("ci_ref") or head_sha or (draft_artifact or {}).get("head_sha") or "").strip()
    if not draft_artifact:
        return {
            "status": "not_measured",
            "state": "unknown",
            "measured": False,
            "repo": repo,
            "ref": ref,
            "warnings": ["CI was not measured because no draft branch artifact exists."],
        }
    if not repo:
        return {
            "status": "not_measured",
            "state": "unknown",
            "measured": False,
            "repo": None,
            "ref": ref,
            "warnings": ["CI was not measured because HERMES_DEV_LAB_CI_REPO is not configured."],
        }
    if not ref:
        return {
            "status": "not_measured",
            "state": "unknown",
            "measured": False,
            "repo": repo,
            "ref": None,
            "warnings": ["CI was not measured because the draft artifact has no head SHA."],
        }
    try:
        payload = (fetcher or fetch_ci_status)(repo=repo, ref=ref)
    except Exception as exc:  # noqa: BLE001 - CI is advisory in the lab loop.
        payload = {"state": "unknown", "warnings": [f"CI status unavailable: {exc}"]}
    state = str((payload or {}).get("state") or "unknown").strip().lower()
    return {
        "status": state if state != "unknown" else "not_measured",
        "state": state,
        "measured": state != "unknown",
        "source": "github",
        "repo": repo,
        "ref": ref,
        "raw": payload,
        "warnings": (payload or {}).get("warnings") or [],
    }


def _measure_code_review_r4(
    *,
    candidate: dict[str, Any],
    context: dict[str, Any],
    plan: dict[str, Any],
    task: dict[str, Any],
    draft_artifact: Optional[dict[str, Any]],
    implementation: dict[str, Any],
    verification: dict[str, Any],
) -> dict[str, Any]:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    fixture = payload.get("code_review_result")
    if fixture is not None:
        parsed = parse_code_review_result(fixture)
        return {
            **parsed,
            "status": parsed.get("verdict") if parsed.get("verdict") != "unknown" else "not_measured",
            "measured": parsed.get("verdict") != "unknown",
            "source": "fixture",
            "output_contract_score": _code_review_contract_score(parsed),
        }
    if not draft_artifact or not draft_artifact.get("ready"):
        return _unmeasured_code_review("Code review was not measured because no draft PR artifact exists.")
    pr_url = str(draft_artifact.get("pr_url") or "").strip()
    pr_number = _pr_number_from_url(pr_url)
    publish = draft_artifact.get("publish") if isinstance(draft_artifact.get("publish"), dict) else {}
    repo = str(payload.get("code_review_repo") or publish.get("repo") or payload.get("ci_repo") or "").strip()
    if not repo or not pr_number:
        return _unmeasured_code_review("Code review was not measured because the draft PR repo or number is unavailable.")
    bridge = context.get("bridge")
    if bridge is None:
        return _unmeasured_code_review("Code review was not measured because no lab worker bridge is configured.")
    pr_state = {
        "repo": repo,
        "pr_number": pr_number,
        "pr_url": pr_url,
        "head_sha": draft_artifact.get("head_sha") or implementation.get("head_sha"),
        "ci_state": verification.get("ci_state") or "unknown",
    }
    prompt = build_code_review_prompt(plan=plan or {}, pr_state=pr_state)
    prompt = "\n".join([
        prompt,
        "",
        "Lab R4 constraints:",
        "- Review only. Do not edit files, commit, push, merge, approve, or publish.",
        "- Use the draft PR diff as the evidence source.",
        "- Do not invoke slash commands or interactive review modes; answer directly.",
        "- If there are no blocking findings, return verdict approved.",
        "- If you cannot inspect the PR, return verdict commented with a finding explaining why.",
        "- Your final response must include the fenced DEV_CODE_REVIEW_RESULT JSON block, then stop.",
    ])
    timeout_seconds = min(max(float(context.get("max_seconds") or 600.0) / 2.0, 60.0), 600.0)
    session = None
    try:
        session = _spawn_lab_review_worker(
            bridge=bridge,
            candidate=candidate,
            project_id=_lab_project_id(candidate),
            prompt=prompt,
            branch=None,
        )
        terminal = _await_review_terminal(
            bridge=bridge,
            runtime="ao",
            session=session,
            timeout_seconds=timeout_seconds,
        )
        transcript = terminal.get("transcript") or ""
        parsed = parse_code_review_result(transcript)
        status = "completed" if parsed.get("verdict") != "unknown" else "needs_attention"
        return {
            **parsed,
            "status": status,
            "measured": parsed.get("verdict") != "unknown",
            "source": "ao",
            "review_session_id": _session_value(session, "id"),
            "review_status": terminal.get("status"),
            "timed_out": terminal.get("timed_out", False),
            "repo": repo,
            "pr_number": pr_number,
            "head_sha": pr_state.get("head_sha"),
            "prompt": prompt,
            "output_contract_score": _code_review_contract_score(parsed),
            "workspace_path": _session_value(terminal.get("session"), "workspace_path") or _session_value(session, "workspace_path"),
            "cleanup": _cleanup_lab_worktree(
                _session_value(terminal.get("session"), "workspace_path") or _session_value(session, "workspace_path")
            ),
        }
    except Exception as exc:  # noqa: BLE001 - review is measured as unavailable, never a pass constant.
        if session is not None:
            _cleanup_lab_worktree(_session_value(session, "workspace_path"))
        return _unmeasured_code_review(f"Code review unavailable: {exc}")


def _unmeasured_code_review(warning: str) -> dict[str, Any]:
    return {
        "object": "hermes.dev_code_review_result",
        "status": "not_measured",
        "verdict": "unknown",
        "findings": [],
        "summary": "",
        "evidence_refs": [],
        "warnings": [warning],
        "measured": False,
        "output_contract_score": None,
    }


def _code_review_contract_score(review: dict[str, Any]) -> float:
    if str(review.get("verdict") or "unknown").lower() == "unknown":
        return 0.0
    score = 0.4
    score += 0.3 if str(review.get("summary") or "").strip() else 0.0
    score += 0.2 if isinstance(review.get("evidence_refs"), list) and review.get("evidence_refs") else 0.0
    score += 0.1 if isinstance(review.get("findings"), list) else 0.0
    if review.get("warnings"):
        score = min(score, 0.75)
    return round(max(0.0, min(1.0, score)), 3)


def _pr_number_from_url(url: str) -> Optional[int]:
    match = re.search(r"/pull/(\d+)(?:\D*$|$)", str(url or ""))
    return int(match.group(1)) if match else None


def _spawn_lab_review_worker(
    *,
    bridge: Any,
    candidate: dict[str, Any],
    project_id: str,
    prompt: str,
    branch: str,
) -> Any:
    kwargs = {
        "project_id": project_id,
        "prompt": prompt,
        "issue_id": None,
        "branch": branch or None,
        "agent": None,
        "model": None,
        "reasoning_effort": "high",
        "minimal_worker_prompt": True,
    }
    try:
        return bridge.spawn("ao", **kwargs)
    except TypeError:
        return bridge.spawn(**kwargs)


def _await_review_terminal(*, bridge: Any, runtime: str, session: Any, timeout_seconds: float) -> dict[str, Any]:
    session_id = str(_session_value(session, "id") or "").strip()
    deadline = time.monotonic() + max(1.0, float(timeout_seconds or 1.0))
    latest = session
    while True:
        status = _session_status(latest)
        if status in set(TASK_COMPLETED_STATUSES) | {"done", "completed"}:
            return {
                "status": "completed",
                "session": latest,
                "transcript": _capture_lab_output(bridge, runtime, latest),
                "timed_out": False,
            }
        if status in set(TASK_FAILED_STATUSES) | {"killed", "errored", "terminated"}:
            return {
                "status": "failed",
                "session": latest,
                "transcript": _capture_lab_output(bridge, runtime, latest),
                "timed_out": False,
            }
        if time.monotonic() >= deadline:
            return {
                "status": "timed_out",
                "session": latest,
                "transcript": _capture_lab_output(bridge, runtime, latest),
                "timed_out": True,
            }
        time.sleep(1.0)
        try:
            refreshed = bridge.status(runtime, session_id)
        except TypeError:
            refreshed = bridge.status(session_id)
        except Exception:
            refreshed = None
        latest = refreshed or latest


def _capture_lab_output(bridge: Any, runtime: str, session: Any) -> str:
    try:
        return bridge.capture_output(runtime, session, lines=240) or ""
    except TypeError:
        return bridge.capture_output(session, lines=240) or ""
    except Exception:
        return ""


def finalize_pending_lab_ci_outcomes(
    *,
    db_path: Path,
    fetcher: Optional[Callable[..., dict[str, Any]]] = None,
    limit: int = 50,
    now: Optional[float] = None,
) -> dict[str, Any]:
    """Refresh CI for draft-PR lab outcomes that were recorded before checks settled."""
    reliability_store = DevReliabilityStore(db_path)
    refreshed_at = float(now or time.time())
    refreshed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for outcome in reliability_store.list_outcomes(limit=limit):
        source_refs = outcome.get("source_refs") if isinstance(outcome.get("source_refs"), dict) else {}
        if source_refs.get("source") != "dogfood_lab_loop" or not source_refs.get("draft_pr_only"):
            continue
        current_state = str(outcome.get("ci_state") or "unknown").strip().lower()
        if current_state != "pending":
            skipped.append({
                "outcome_id": outcome.get("outcome_id"),
                "reason": f"ci_state_not_pending:{current_state}",
            })
            continue
        ci_status_ref = source_refs.get("ci_status") if isinstance(source_refs.get("ci_status"), dict) else {}
        draft_artifact = source_refs.get("draft_artifact") if isinstance(source_refs.get("draft_artifact"), dict) else {}
        publish = draft_artifact.get("publish") if isinstance(draft_artifact.get("publish"), dict) else {}
        repo = str(ci_status_ref.get("repo") or publish.get("repo") or "").strip()
        ref = str(ci_status_ref.get("ref") or source_refs.get("head_sha") or draft_artifact.get("head_sha") or "").strip()
        if not repo or not ref:
            skipped.append({
                "outcome_id": outcome.get("outcome_id"),
                "reason": "missing_repo_or_ref",
                "repo": repo or None,
                "ref": ref or None,
            })
            continue
        try:
            raw_ci = (fetcher or fetch_ci_status)(repo=repo, ref=ref)
        except Exception as exc:  # noqa: BLE001 - CI refresh is advisory and should not break the loop.
            raw_ci = {"state": "unknown", "warnings": [f"CI status unavailable: {exc}"]}
        next_state = str((raw_ci or {}).get("state") or "unknown").strip().lower()
        measured = next_state != "unknown"
        next_status = next_state if measured else "not_measured"
        next_source_refs = {
            **source_refs,
            "ci_status": {
                "status": next_status,
                "state": next_state,
                "measured": measured,
                "source": "github",
                "repo": repo,
                "ref": ref,
                "raw": raw_ci,
                "warnings": (raw_ci or {}).get("warnings") or [],
                "refreshed_at": refreshed_at,
            },
            "ci_finalized_at": refreshed_at if next_state in {"success", "failure"} else None,
        }
        gates = dict(next_source_refs.get("gates") or {})
        gates["ci"] = next_status
        next_source_refs["gates"] = gates
        next_terminal_status = "failed" if next_state == "failure" else outcome.get("terminal_status")
        updated = reliability_store.upsert_outcome({
            **outcome,
            "terminal_status": next_terminal_status,
            "ci_state": next_state if measured else current_state,
            "source_refs": next_source_refs,
            "updated_at": refreshed_at,
        })
        refreshed.append({
            "outcome_id": updated.get("outcome_id"),
            "previous_ci_state": current_state,
            "ci_state": updated.get("ci_state"),
            "terminal_status": updated.get("terminal_status"),
            "success": updated.get("success"),
            "repo": repo,
            "ref": ref,
            "warnings": (raw_ci or {}).get("warnings") or [],
        })
    return {
        "object": "hermes.dev_lab_ci_finalization",
        "refreshed_at": refreshed_at,
        "refreshed": refreshed,
        "skipped": skipped,
        "counts": {
            "refreshed": len(refreshed),
            "skipped": len(skipped),
        },
    }


def _task_branch(candidate: dict[str, Any]) -> str:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    branch = str(payload.get("branch") or "").strip()
    if branch:
        return branch
    slug = "".join(ch if ch.isalnum() else "-" for ch in str(candidate.get("candidate_id") or uuid.uuid4().hex[:8]).lower())
    slug = "-".join(part for part in slug.split("-") if part)[:48] or uuid.uuid4().hex[:8]
    return f"lab/dogfood/{slug}"


def _lab_project_id(candidate: dict[str, Any]) -> str:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    if payload.get("project_id"):
        return str(payload["project_id"])
    paths = [str(path) for path in candidate.get("target_paths") or []]
    if any(path.startswith("apps/oryn-workspace/") for path in paths):
        return "OrynWorkspaceLab"
    if any(path.startswith(("bootstrap/", "hermes-ops/")) for path in paths):
        return "OrynPlatformLab"
    return "HermesAgentLab"


def _lab_worker_prompt(candidate: dict[str, Any], acceptance_criteria: list[Any]) -> str:
    criteria_lines = [f"- {item}" for item in acceptance_criteria] if acceptance_criteria else ["- Produce a scoped, reviewable harness change."]
    return "\n".join([
        str(candidate.get("prompt") or "Improve the Hermes harness in the scoped target paths.").strip(),
        "",
        "Lab constraints:",
        "- Edit only the listed target paths.",
        "- Do not touch Hermes engine paths such as agent/ or agent/transports/.",
        "- Do not merge, publish, release, restart stable services, or mutate production checkouts.",
        "- Commit the change on the provided lab branch when edits are complete.",
        "- If no change is appropriate, report that clearly instead of editing unrelated files.",
        "",
        "Target paths:",
        *[f"- {path}" for path in candidate.get("target_paths") or []],
        "",
        "Acceptance criteria:",
        *criteria_lines,
    ]).strip()


def _worker_timeout_seconds(context: dict[str, Any]) -> float:
    configured = _env_float("HERMES_DEV_LAB_WORKER_TIMEOUT_SECONDS", 900.0)
    pass_budget = float(context.get("max_seconds") or configured)
    elapsed = max(0.0, time.time() - float(context.get("started_at") or time.time()))
    remaining = max(1.0, pass_budget - elapsed)
    return min(configured, remaining)


def _apply_adversarial_diff_fixture(
    *,
    candidate: dict[str, Any],
    workspace_path: Any,
    enabled: bool,
) -> dict[str, Any]:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    requested_paths = [
        normalize_target_path(path)
        for path in payload.get("adversarial_diff_paths") or []
        if normalize_target_path(path)
    ]
    if not requested_paths:
        return {"requested": False, "enabled": bool(enabled), "applied": False, "paths": [], "warnings": []}
    result: dict[str, Any] = {
        "requested": True,
        "enabled": bool(enabled),
        "applied": False,
        "paths": [],
        "warnings": [],
    }
    if not enabled:
        result["warnings"].append("Adversarial diff fixture was requested but not enabled for this lab pass.")
        return result
    workspace = Path(str(workspace_path or "")).expanduser()
    if not workspace.exists() or not workspace.is_dir():
        result["warnings"].append("Adversarial diff fixture could not run because the worker workspace is missing.")
        return result
    workspace_root = workspace.resolve(strict=False)
    for rel_path in requested_paths:
        if rel_path.startswith("/") or ".." in Path(rel_path).parts:
            result["warnings"].append(f"Rejected unsafe adversarial fixture path: {rel_path}")
            continue
        target = (workspace / rel_path).resolve(strict=False)
        if workspace_root != target and workspace_root not in target.parents:
            result["warnings"].append(f"Rejected adversarial fixture path outside workspace: {rel_path}")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        existing = target.read_text(encoding="utf-8") if target.exists() else ""
        suffix = "\n" if existing and not existing.endswith("\n") else ""
        target.write_text(
            f"{existing}{suffix}# HERMES LAB ADVERSARIAL DIFF FIXTURE: {uuid.uuid4().hex[:8]}\n",
            encoding="utf-8",
        )
        result["paths"].append(rel_path)
    result["applied"] = bool(result["paths"])
    return result


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _diff_scope_result(touched_paths: list[str]) -> dict[str, Any]:
    if not touched_paths:
        return {"ok": True, "status": "empty_diff", "target_paths": [], "rejected_paths": []}
    return dogfood_scope_check(touched_paths)


def _touched_paths_from_worktree(
    workspace_path: Any,
    *,
    fallback_paths: Optional[list[Any]] = None,
    base_ref: Optional[str] = None,
) -> list[str]:
    paths: list[str] = []
    workspace_text = str(workspace_path or "").strip()
    if workspace_text:
        workspace = Path(workspace_text).expanduser()
    else:
        workspace = None
    if workspace and workspace.exists() and workspace.is_dir():
        for command in _git_diff_commands(base_ref):
            paths.extend(_git_lines(workspace, command))
        paths.extend(_paths_from_porcelain(_git_lines(workspace, ["status", "--porcelain=v1"])))
    if not paths:
        paths.extend(str(path) for path in (fallback_paths or []))
    return _drop_parent_paths(_unique_paths(_non_bootstrap_diff_paths(paths)))


def _non_bootstrap_diff_paths(paths: list[str]) -> list[str]:
    filtered: list[str] = []
    for path in paths:
        normalized = str(path or "").strip().replace("\\", "/").strip("/")
        if not normalized:
            continue
        first = normalized.split("/", 1)[0]
        if first in DIFF_SCOPE_IGNORED_PATHS or normalized in DIFF_SCOPE_IGNORED_PATHS:
            continue
        filtered.append(normalized)
    return filtered


def _drop_parent_paths(paths: list[str]) -> list[str]:
    ordered = list(paths or [])
    path_set = set(ordered)
    return [
        path for path in ordered
        if not any(other != path and other.startswith(f"{path.rstrip('/')}/") for other in path_set)
    ]


def _git_diff_commands(base_ref: Optional[str]) -> list[list[str]]:
    commands: list[list[str]] = []
    if base_ref:
        commands.append(["diff", "--name-only", f"{base_ref}...HEAD"])
    commands.extend([
        ["diff", "--name-only", "@{upstream}...HEAD"],
        ["diff", "--name-only", "origin/main...HEAD"],
        ["diff", "--name-only", "HEAD"],
        ["diff", "--cached", "--name-only"],
        ["ls-files", "--others", "--modified", "--exclude-standard"],
    ])
    return commands


def _git_lines(workspace: Path, args: list[str]) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(workspace), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    return [line.rstrip() for line in result.stdout.splitlines() if line.strip()]


def _run_command(args: list[str], *, timeout: float = 30.0) -> dict[str, Any]:
    try:
        result = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001 - command failures are carried as evidence.
        return {"returncode": 1, "stdout": "", "stderr": str(exc)}
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _paths_from_porcelain(lines: list[str]) -> list[str]:
    paths: list[str] = []
    for line in lines:
        text = line[3:] if len(line) > 3 else line
        if " -> " in text:
            text = text.split(" -> ", 1)[1]
        if text:
            paths.append(text)
    return paths


def _unique_paths(paths: list[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for path in paths:
        normalized = str(path or "").strip().replace("\\", "/")
        while normalized.startswith("./"):
            normalized = normalized[2:]
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return _drop_parent_directory_entries(result)


def _drop_parent_directory_entries(paths: list[str]) -> list[str]:
    result: list[str] = []
    for path in paths:
        if path.endswith("/") and any(other != path and other.startswith(path) for other in paths):
            continue
        result.append(path)
    return result


def _draft_branch_artifact(
    *,
    candidate: dict[str, Any],
    implementation: dict[str, Any],
    workspace_path: Any,
) -> dict[str, Any]:
    branch = str(implementation.get("branch") or "").strip()
    head_sha = implementation.get("head_sha") or _git_head_sha(workspace_path)
    pr = implementation.get("pr") if isinstance(implementation.get("pr"), dict) else None
    publish = _publish_lab_draft_pr(candidate=candidate, workspace_path=workspace_path, branch=branch)
    if publish.get("ready"):
        pr = {
            "url": publish.get("pr_url"),
            "html_url": publish.get("pr_url"),
            "number": publish.get("pr_number"),
        }
        head_sha = publish.get("head_sha") or head_sha
    return {
        "ready": True,
        "type": "draft_pr" if pr else "local_branch",
        "branch": branch,
        "head_sha": head_sha,
        "pr_url": (pr or {}).get("url") or (pr or {}).get("html_url"),
        "pr_number": (pr or {}).get("number"),
        "draft": True,
        "publish": publish,
    }


def _publish_lab_draft_pr(*, candidate: dict[str, Any], workspace_path: Any, branch: str) -> dict[str, Any]:
    config = _lab_draft_pr_config(candidate)
    if not config.get("enabled"):
        return {
            "ready": False,
            "status": "not_configured",
            "warnings": [config.get("reason") or "Lab draft PR publishing is not configured."],
        }
    workspace = Path(str(workspace_path or "")).expanduser()
    if not workspace.exists():
        return {
            "ready": False,
            "status": "failed",
            "warnings": [f"Workspace path does not exist: {workspace}"],
        }
    remote = config["remote"]
    repo = config["repo"]
    base = config["base"]
    head = config.get("head") or branch
    title = str(candidate.get("prompt") or "Hermes Lab dogfood task").strip().splitlines()[0][:120]
    body = "\n".join([
        "Hermes Lab dogfood draft PR.",
        "",
        "Safety posture:",
        "- lab-generated branch",
        "- draft PR only",
        "- no merge or publish authority",
    ])
    push = _run_command(["git", "-C", str(workspace), "push", remote, f"HEAD:{branch}"], timeout=60)
    if push["returncode"] != 0:
        return {
            "ready": False,
            "status": "push_failed",
            "repo": repo,
            "remote": remote,
            "branch": branch,
            "warnings": [push.get("stderr") or push.get("stdout") or "git push failed"],
        }
    pr = _run_command(
        [
            "gh",
            "pr",
            "create",
            "--draft",
            "--repo",
            repo,
            "--base",
            base,
            "--head",
            head,
            "--title",
            title,
            "--body",
            body,
        ],
        timeout=60,
    )
    if pr["returncode"] != 0:
        return {
            "ready": False,
            "status": "pr_create_failed",
            "repo": repo,
            "remote": remote,
            "branch": branch,
            "head": head,
            "warnings": [pr.get("stderr") or pr.get("stdout") or "gh pr create failed"],
        }
    head_sha = _git_head_sha(workspace)
    pr_url = (pr.get("stdout") or "").strip().splitlines()[-1] if (pr.get("stdout") or "").strip() else None
    return {
        "ready": True,
        "status": "created",
        "repo": repo,
        "remote": remote,
        "base": base,
        "head": head,
        "branch": branch,
        "head_sha": head_sha,
        "pr_url": pr_url,
        "warnings": [],
    }


def _lab_draft_pr_config(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    repo = str(
        payload.get("draft_pr_repo")
        or payload.get("ci_repo")
        or os.getenv("HERMES_DEV_LAB_DRAFT_PR_REPO")
        or os.getenv("HERMES_DEV_LAB_CI_REPO")
        or ""
    ).strip()
    remote = str(payload.get("draft_pr_remote") or os.getenv("HERMES_DEV_LAB_DRAFT_PR_REMOTE") or "").strip()
    base = str(payload.get("draft_pr_base") or os.getenv("HERMES_DEV_LAB_DRAFT_PR_BASE") or "main").strip()
    head = str(payload.get("draft_pr_head") or os.getenv("HERMES_DEV_LAB_DRAFT_PR_HEAD") or "").strip()
    if not repo:
        return {"enabled": False, "reason": "Lab draft PR repo is not configured."}
    if not remote:
        return {"enabled": False, "repo": repo, "reason": "Lab draft PR remote is not configured."}
    return {"enabled": True, "repo": repo, "remote": remote, "base": base or "main", "head": head}


def _lab_terminal_status(
    *,
    implementation: dict[str, Any],
    verification_verdict: str,
    quarantined: bool,
    empty_diff: bool,
) -> str:
    if quarantined or empty_diff or implementation.get("status") != "completed":
        return "failed"
    if verification_verdict == "verified":
        return "completed"
    if verification_verdict in {"failed", "partial", "needs_review"}:
        return "failed"
    return "needs_attention"


def _execution_excluded_from_scorecard(implementation: dict[str, Any]) -> bool:
    if bool(implementation.get("invalid_outcome") or implementation.get("scorecard_excluded")):
        return True
    status = str(implementation.get("status") or "").lower()
    reason = str(implementation.get("reason") or "").lower()
    if status in {"runner_aborted", "premature_terminal", "invalid"}:
        return True
    return "premature_terminal" in reason or "runner_defect" in reason


def _git_head_sha(workspace_path: Any) -> Optional[str]:
    workspace = Path(str(workspace_path or "")).expanduser()
    if not workspace.exists() or not workspace.is_dir():
        return None
    lines = _git_lines(workspace, ["rev-parse", "HEAD"])
    return lines[0] if lines else None


def _cleanup_lab_worktree(workspace_path: Any) -> dict[str, Any]:
    workspace = Path(str(workspace_path or "")).expanduser()
    if not workspace.exists() or not workspace.is_dir():
        return {"cleaned": False, "reason": "missing"}
    lab_worktrees = Path(lab_paths_from_env()["worktrees_dir"]).expanduser().resolve(strict=False)
    lab_ao_worktrees = Path(lab_paths_from_env()["lab_home"]).expanduser().resolve(strict=False) / ".worktrees"
    resolved = workspace.resolve(strict=False)
    if not (
        resolved == lab_worktrees
        or lab_worktrees in resolved.parents
        or resolved == lab_ao_worktrees
        or lab_ao_worktrees in resolved.parents
    ):
        return {"cleaned": False, "reason": "outside_lab_worktrees", "workspace_path": str(workspace)}
    result = subprocess.run(
        ["git", "-C", str(workspace), "worktree", "remove", "--force", str(workspace)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        shutil.rmtree(workspace, ignore_errors=True)
    return {"cleaned": not workspace.exists(), "workspace_path": str(workspace)}


def _first_numeric(*values: Any) -> Optional[float]:
    for value in values:
        try:
            if value is not None and str(value).strip() != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name) or default)
    except (TypeError, ValueError):
        return default


def loop_health(*, db_path: Path) -> dict[str, Any]:
    loop_store = DevLabLoopStore(db_path)
    reliability_store = DevReliabilityStore(db_path)
    signal_store = DevProductionSignalStore(db_path)
    outcomes = reliability_store.list_outcomes(limit=5000)
    real_outcomes = [
        item for item in outcomes
        if not ((item.get("source_refs") or {}).get("seeded"))
        and not outcome_excluded(item)
    ]
    card = scorecard(outcomes)
    proposals = signal_store.list_proposals(limit=200)
    return {
        "ok": True,
        "object": "hermes.dev_lab_loop_health",
        "state": loop_store.get_state(),
        "recent_passes": loop_store.list_passes(limit=10),
        "pending_candidates": loop_store.list_candidates(approved=True, limit=20),
        "real_outcome_count": len(real_outcomes),
        "outcome_count_by_category": _count_by(real_outcomes, "category"),
        "scorecard_summary": card.get("summary") or {},
        "weakest": card.get("weakest") or [],
        "proposal_counts": _count_by(proposals, "status"),
        "open_proposal_count": sum(1 for proposal in proposals if proposal.get("status") in {"proposed", "approved"}),
        "advisory_only": True,
    }


def enqueue_candidates(store: DevLabLoopStore, candidates: list[dict[str, Any]], *, auto_approve: bool = False) -> list[dict[str, Any]]:
    queued = []
    for candidate in candidates:
        approved = auto_approve or preapproval_allows(candidate) or bool(candidate.get("approved"))
        queued.append(store.upsert_candidate(candidate, approved=approved))
    return queued


def enqueue_approved_proposals(*, db_path: Path, store: Optional[DevLabLoopStore] = None, limit: int = 50) -> list[dict[str, Any]]:
    """Append human-approved proposals to the dogfood backlog without promoting them."""

    loop_store = store or DevLabLoopStore(db_path)
    signal_store = DevProductionSignalStore(db_path)
    queued: list[dict[str, Any]] = []
    for proposal in signal_store.list_proposals(status="approved", limit=limit):
        payload = proposal.get("payload") or {}
        target_category = payload.get("target_category") or (proposal.get("query_descriptor") or {}).get("category")
        candidate = {
            "candidate_id": f"dogfood:proposal:{proposal.get('proposal_id')}",
            "prompt": payload.get("suggested_change") or payload.get("title") or proposal.get("cluster_key"),
            "profile_id": "platform.implement",
            "risk_level": "medium" if payload.get("guardrail_touching") else "low",
            "target_paths": _target_paths_for_proposal(payload),
            "source": str(payload.get("source") or "proposal"),
            "approved": True,
            "guardrail_touching": bool(payload.get("guardrail_touching")),
            "payload": {
                "proposal_id": proposal.get("proposal_id"),
                "cluster_key": proposal.get("cluster_key"),
                "target_category": target_category,
                "evidence_refs": proposal.get("evidence_refs") or [],
            },
        }
        queued.append(loop_store.upsert_candidate(candidate, approved=True))
    return queued


def _run_digest(db_path: Path, *, sources: Optional[list[str]]) -> dict[str, Any]:
    selected = sources or [source.strip() for source in os.getenv("HERMES_DEV_SIGNAL_DIGEST_SOURCES", "deterministic,product,reliability").split(",") if source.strip()]
    return run_signal_digest_sources(
        signal_store=DevProductionSignalStore(db_path),
        event_store=SubagentEventStore(db_path),
        product_event_store=DevProductEventStore(db_path),
        reliability_store=DevReliabilityStore(db_path),
        execution_store=DevExecutionStore(db_path),
        sources=selected,
        window_days=7,
        persist=True,
    )


def _target_paths_for_proposal(payload: dict[str, Any]) -> list[str]:
    category = str(payload.get("target_category") or payload.get("category") or "").lower()
    if "workspace" in category:
        return ["apps/oryn-workspace/", "tests/"]
    if any(term in category for term in ("docs", "documentation")):
        return ["docs/"]
    return ["gateway/dev_control/", "tests/"]


def _apply_breakers(
    report: dict[str, Any],
    store: DevLabLoopStore,
    *,
    max_consecutive_failures: int,
    max_consecutive_out_of_scope: int,
    max_seconds: float = 1800.0,
    max_cost_usd: Optional[float] = None,
    regression_threshold: float = 0.20,
) -> None:
    if report.get("breaker_reason"):
        report["status"] = "loop_halted"
        report["ok"] = False
        return
    state = store.get_state()
    elapsed = float(report.get("completed_at") or time.time()) - float(report.get("started_at") or time.time())
    after_failure_count = int(state.get("consecutive_failure_count") or 0) + (1 if report.get("status") == "failed" else 0)
    after_skip_count = int(state.get("consecutive_out_of_scope_count") or 0) + (
        1 if report.get("status") == "skipped" and report.get("skip_reason") == "out_of_scope" else 0
    )
    if after_failure_count >= max_consecutive_failures:
        report["breaker_reason"] = f"consecutive_failures:{after_failure_count}"
    elif after_skip_count >= max_consecutive_out_of_scope:
        report["breaker_reason"] = f"consecutive_out_of_scope:{after_skip_count}"
    elif elapsed > max_seconds:
        report["breaker_reason"] = f"time_budget_exceeded:{elapsed:.1f}s"
    elif max_cost_usd is not None and float((report.get("execution") or {}).get("cost_usd") or 0.0) > float(max_cost_usd):
        report["breaker_reason"] = f"cost_budget_exceeded:{float((report.get('execution') or {}).get('cost_usd') or 0.0):.4f}"
    else:
        before = float((report.get("scorecard_before") or {}).get("average_success_rate") or 0.0)
        after = float((report.get("scorecard_after") or {}).get("average_success_rate") or before)
        if before > 0 and before - after >= regression_threshold:
            report["breaker_reason"] = f"scorecard_regression:{before:.2f}->{after:.2f}"
    if report.get("breaker_reason"):
        report["status"] = "loop_halted"
        report["ok"] = False


def _base_report(started: float, status: str, *, candidate: Optional[dict[str, Any]]) -> dict[str, Any]:
    now = time.time()
    return {
        "ok": status in {"completed", "idle", "skipped"},
        "object": "hermes.dev_lab_loop_pass",
        "pass_id": f"devlab-pass-{uuid.uuid4().hex[:10]}",
        "status": status,
        "candidate_id": (candidate or {}).get("candidate_id"),
        "candidate": candidate,
        "started_at": started,
        "completed_at": now,
        "observe_mode": True,
        "draft_pr_only": True,
        "merge_executed": False,
        "publish_executed": False,
    }


def _scorecard_summary(card: dict[str, Any]) -> dict[str, Any]:
    categories = card.get("categories") or []
    rates = [float(row.get("success_rate") or 0.0) for row in categories]
    return {
        **(card.get("summary") or {}),
        "average_success_rate": round(sum(rates) / len(rates), 4) if rates else None,
    }


def _mtime(path: Optional[Path]) -> float | None:
    if not path:
        return None
    candidate = Path(path).expanduser()
    return candidate.stat().st_mtime if candidate.exists() else None


def _attach_isolation_fields(
    report: dict[str, Any],
    *,
    stable_before: float | None,
    stable_db_path: Optional[Path],
    isolation_pids: Optional[list[int | str]],
) -> None:
    report["stable_db_telemetry"] = _stable_db_telemetry(stable_before, stable_db_path)
    # Backward-compatible field only. This is no longer an isolation gate.
    report["stable_db_unchanged"] = report["stable_db_telemetry"]["unchanged"]
    report["isolation"] = _audit_lab_isolation(isolation_pids)
    if _has_forbidden_root_write(report["isolation"]):
        report["breaker_reason"] = "isolation_breach"
        report["status"] = "loop_halted"
        report["ok"] = False


def _has_forbidden_root_write(isolation: dict[str, Any]) -> bool:
    return any(bool(item.get("in_forbidden_root")) for item in (isolation or {}).get("offending_paths") or [])


def _stable_db_telemetry(before: float | None, path: Optional[Path]) -> dict[str, Any]:
    after = _mtime(path)
    return {
        "authoritative": False,
        "signal": "mtime",
        "path": str(path.expanduser()) if path else None,
        "before": before,
        "after": after,
        "unchanged": before == after,
        "note": "Stable DB mtime is informational only; live stable services may update WAL/mtime independently of lab.",
    }


def _audit_lab_isolation(extra_pids: Optional[list[int | str]] = None) -> dict[str, Any]:
    env_pids = [
        value.strip()
        for value in str(os.getenv("HERMES_DEV_LAB_GATEWAY_PID") or "").split(",")
        if value.strip()
    ]
    try:
        return audit_current_process_isolation([*(extra_pids or []), *env_pids])
    except Exception as exc:  # noqa: BLE001 - failures become a hard stop in the report.
        return {
            "ok": False,
            "object": "hermes.dev_lab_process_isolation",
            "pids": [os.getpid(), *(extra_pids or []), *env_pids],
            "write_handles": [],
            "offending_paths": [],
            "warnings": [f"Lab process isolation audit failed: {exc}"],
            "authoritative": True,
        }


def _candidate_values(candidate: dict[str, Any]) -> tuple[Any, ...]:
    return (
        candidate["candidate_id"],
        candidate["prompt"],
        candidate["profile_id"],
        candidate["risk_level"],
        _json(candidate.get("target_paths") or []),
        candidate["source"],
        1 if candidate.get("approved") else 0,
        candidate["status"],
        candidate["scope_status"],
        _json(candidate.get("scope_warnings") or []),
        1 if candidate.get("guardrail_touching") else 0,
        float(candidate["created_at"]),
        float(candidate["updated_at"]),
        _json(candidate.get("payload") or {}),
    )


def _candidate_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "object": "hermes.dev_lab_dogfood_candidate",
        "candidate_id": row["candidate_id"],
        "prompt": row["prompt"],
        "profile_id": row["profile_id"],
        "risk_level": row["risk_level"],
        "target_paths": _loads(row["target_paths"], []),
        "source": row["source"],
        "approved": bool(row["approved"]),
        "status": row["status"],
        "scope_status": row["scope_status"],
        "scope_warnings": _loads(row["scope_warnings"], []),
        "guardrail_touching": bool(row["guardrail_touching"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payload": _loads(row["payload"], {}),
    }


def _pass_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return _loads(row["report"], {})


def _state_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "object": "hermes.dev_lab_loop_state",
        "status": row["status"],
        "halted_reason": row["halted_reason"],
        "consecutive_failure_count": int(row["consecutive_failure_count"] or 0),
        "consecutive_out_of_scope_count": int(row["consecutive_out_of_scope_count"] or 0),
        "last_pass_id": row["last_pass_id"],
        "updated_at": row["updated_at"],
        "payload": _loads(row["payload"], {}),
    }


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: Any, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return default
