"""DeepSWE Lab benchmark evidence, diagnosis, and advisory actions."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from gateway.dev_control.lab_environment import lab_paths_from_env, validate_lab_or_raise
from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_deepswe_benchmark_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    mode TEXT NOT NULL,
    provider TEXT NOT NULL,
    pinned_context TEXT NOT NULL,
    task_results TEXT NOT NULL,
    summary TEXT NOT NULL,
    command TEXT NOT NULL,
    infrastructure_failure TEXT NOT NULL,
    artifact_refs TEXT NOT NULL,
    blockers TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_deepswe_runs_created
    ON dev_deepswe_benchmark_runs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_deepswe_runs_status
    ON dev_deepswe_benchmark_runs(status, created_at DESC);

CREATE TABLE IF NOT EXISTS dev_deepswe_diagnoses (
    diagnosis_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    patterns TEXT NOT NULL,
    exploratory INTEGER NOT NULL,
    created_at REAL NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_deepswe_diagnoses_run
    ON dev_deepswe_diagnoses(run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS dev_deepswe_actions (
    action_id TEXT PRIMARY KEY,
    diagnosis_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    action_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    proposal_id TEXT,
    lab_candidate_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_deepswe_actions_diagnosis
    ON dev_deepswe_actions(diagnosis_id, created_at DESC);
"""

REQUIRED_CONTEXT_FIELDS = {
    "deepswe_repo_url",
    "deepswe_commit",
    "pier_version",
    "agent_adapter",
    "model_profile",
}

RAW_BENCHMARK_KEYS = {
    "instruction",
    "instruction_md",
    "prompt",
    "prompts",
    "reference_solution",
    "solution",
    "test_patch",
    "verifier_patch",
    "trajectory",
    "trajectories",
    "transcript",
    "raw_transcript",
    "messages",
    "raw_messages",
    "stdout",
    "stderr",
    "logs",
    "corpus",
}

INFRASTRUCTURE_FAILURE_CATEGORIES = {
    "dependency_environment_failure",
    "missing_image",
    "missing_credentials",
    "pier_failure",
    "verifier_infrastructure_failure",
}

FAILURE_CATEGORIES = {
    "navigation_failure",
    "wrong_file_edit",
    "incomplete_implementation",
    "output_contract_failure",
    "verifier_failure",
    "timeout",
    "dependency_environment_failure",
    "regression",
    "cost_blowout",
}


@dataclass
class DevDeepSWEBenchmarkStore:
    """Persistence for DeepSWE Lab benchmark runs, diagnoses, and actions."""

    db_path: Optional[Path] = None

    def __post_init__(self) -> None:
        self.db_path = self.db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        apply_wal_with_fallback(self._conn, db_label="state.db")
        self._lock = threading.Lock()
        with self._conn:
            self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self._conn.close()

    def create_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        normalized = normalize_deepswe_run({
            **payload,
            "run_id": payload.get("run_id") or f"devdswe-{uuid.uuid4().hex[:10]}",
            "created_at": payload.get("created_at") or now,
            "updated_at": payload.get("updated_at") or now,
        })
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_deepswe_benchmark_runs (
                    run_id, status, mode, provider, pinned_context, task_results,
                    summary, command, infrastructure_failure, artifact_refs,
                    blockers, created_at, updated_at, completed_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _run_values(normalized),
            )
        return self.get_run(normalized["run_id"]) or normalized

    def update_run(self, run_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_run(run_id)
        if current is None:
            raise KeyError(f"DeepSWE benchmark run not found: {run_id}")
        merged = normalize_deepswe_run({**current, **updates, "updated_at": time.time()})
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_deepswe_benchmark_runs
                SET status = ?, mode = ?, provider = ?, pinned_context = ?,
                    task_results = ?, summary = ?, command = ?,
                    infrastructure_failure = ?, artifact_refs = ?, blockers = ?,
                    created_at = ?, updated_at = ?, completed_at = ?, payload = ?
                WHERE run_id = ?
                """,
                (*_run_values(merged)[1:], run_id),
            )
        return self.get_run(run_id) or merged

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_deepswe_benchmark_runs WHERE run_id = ?",
            (str(run_id or "").strip(),),
        ).fetchone()
        return _run_from_row(row) if row else None

    def list_runs(self, *, status: Optional[str] = None, limit: int = 50) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(str(status).strip())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_deepswe_benchmark_runs
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_run_summary_from_row(row) for row in rows]

    def create_diagnosis(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        normalized = {
            "object": "hermes.dev_deepswe_diagnosis",
            "diagnosis_id": str(payload.get("diagnosis_id") or f"devdswe-diag-{uuid.uuid4().hex[:10]}"),
            "run_id": str(payload.get("run_id") or ""),
            "status": str(payload.get("status") or "completed"),
            "patterns": compact_deepswe_evidence(payload.get("patterns") or []),
            "exploratory": bool(payload.get("exploratory")),
            "created_at": float(payload.get("created_at") or now),
        }
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_deepswe_diagnoses (
                    diagnosis_id, run_id, status, patterns, exploratory, created_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["diagnosis_id"],
                    normalized["run_id"],
                    normalized["status"],
                    _json(normalized["patterns"]),
                    1 if normalized["exploratory"] else 0,
                    normalized["created_at"],
                    _json(normalized),
                ),
            )
        return self.get_diagnosis(normalized["diagnosis_id"]) or normalized

    def get_diagnosis(self, diagnosis_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_deepswe_diagnoses WHERE diagnosis_id = ?",
            (str(diagnosis_id or "").strip(),),
        ).fetchone()
        return _diagnosis_from_row(row) if row else None

    def latest_diagnosis(self, run_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM dev_deepswe_diagnoses
            WHERE run_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (str(run_id or "").strip(),),
        ).fetchone()
        return _diagnosis_from_row(row) if row else None

    def create_action(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        normalized = {
            "object": "hermes.dev_deepswe_action",
            "action_id": str(payload.get("action_id") or f"devdswe-act-{uuid.uuid4().hex[:10]}"),
            "diagnosis_id": str(payload.get("diagnosis_id") or ""),
            "run_id": str(payload.get("run_id") or ""),
            "status": str(payload.get("status") or "proposed"),
            "action_type": str(payload.get("action_type") or "plan_brief"),
            "payload": compact_deepswe_evidence(payload.get("payload") or {}),
            "proposal_id": payload.get("proposal_id"),
            "lab_candidate_id": payload.get("lab_candidate_id"),
            "created_at": float(payload.get("created_at") or now),
            "updated_at": float(payload.get("updated_at") or now),
        }
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_deepswe_actions (
                    action_id, diagnosis_id, run_id, status, action_type,
                    payload, proposal_id, lab_candidate_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["action_id"],
                    normalized["diagnosis_id"],
                    normalized["run_id"],
                    normalized["status"],
                    normalized["action_type"],
                    _json(normalized["payload"]),
                    normalized.get("proposal_id"),
                    normalized.get("lab_candidate_id"),
                    normalized["created_at"],
                    normalized["updated_at"],
                ),
            )
        return self.get_action(normalized["action_id"]) or normalized

    def get_action(self, action_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_deepswe_actions WHERE action_id = ?",
            (str(action_id or "").strip(),),
        ).fetchone()
        return _action_from_row(row) if row else None

    def list_actions(self, *, diagnosis_id: Optional[str] = None, run_id: Optional[str] = None, limit: int = 50) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if diagnosis_id:
            clauses.append("diagnosis_id = ?")
            params.append(str(diagnosis_id).strip())
        if run_id:
            clauses.append("run_id = ?")
            params.append(str(run_id).strip())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_deepswe_actions
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_action_from_row(row) for row in rows]


def start_deepswe_benchmark(
    *,
    store: DevDeepSWEBenchmarkStore,
    context: Dict[str, Any],
    task_results: Optional[list[Dict[str, Any]]] = None,
    result_artifact: Optional[Path | str] = None,
    mode: str = "fixture",
    execute: bool = False,
    persist: bool = True,
    runner: Any = None,
) -> Dict[str, Any]:
    pinned_context = normalize_deepswe_context(context)
    blockers = pinned_context.get("blockers") or []
    normalized_mode = _normalize_mode(mode)
    if blockers and not pinned_context.get("exploratory"):
        raise ValueError(f"DeepSWE run context is incomplete or missing: {', '.join(blocker['field'] for blocker in blockers)}")

    command = build_pier_command(pinned_context)
    infrastructure_failure: Dict[str, Any] = {}
    status = "dry_run" if normalized_mode == "dry_run" else "completed"
    raw_results = task_results
    completed_at = time.time()
    artifact_refs = list(pinned_context.get("artifact_refs") or [])

    if result_artifact:
        parsed = parse_deepswe_result_artifact(result_artifact)
        raw_results = parsed.get("task_results") or raw_results or []
        artifact_refs.extend(parsed.get("artifact_refs") or [])

    if execute:
        _validate_real_deepswe_run(pinned_context)
        try:
            completed = _execute_pier_command(
                command,
                cwd=Path(pinned_context["deepswe_checkout_path"]),
                timeout_seconds=float((pinned_context.get("resource_limits") or {}).get("timeout_seconds") or 3600),
                runner=runner,
            )
            raw_results = raw_results or parse_deepswe_result_artifact(completed.get("result_artifact") or {}).get("task_results") or []
            status = "completed" if int(completed.get("returncode") or 0) == 0 else "infrastructure_failed"
            infrastructure_failure = completed.get("infrastructure_failure") or {}
            artifact_refs.extend(completed.get("artifact_refs") or [])
        except TimeoutError as exc:
            status = "infrastructure_failed"
            infrastructure_failure = _infra_failure("timeout", str(exc))
        except Exception as exc:  # noqa: BLE001 - persisted as benchmark infrastructure evidence.
            status = "infrastructure_failed"
            infrastructure_failure = _infra_failure("pier_failure", str(exc))

    parsed_results = parse_deepswe_task_results(raw_results or [])
    summary = summarize_deepswe_results(parsed_results, pinned_context, infrastructure_failure=infrastructure_failure)
    if infrastructure_failure:
        status = "infrastructure_failed"
    payload = {
        "object": "hermes.dev_deepswe_benchmark_run",
        "run_id": context.get("run_id") or f"devdswe-{uuid.uuid4().hex[:10]}",
        "status": status,
        "mode": normalized_mode,
        "provider": "deepswe",
        "pinned_context": pinned_context,
        "task_results": parsed_results,
        "summary": summary,
        "command": command,
        "infrastructure_failure": infrastructure_failure,
        "artifact_refs": compact_deepswe_evidence(artifact_refs),
        "blockers": blockers,
        "created_at": time.time(),
        "updated_at": time.time(),
        "completed_at": completed_at,
    }
    if not persist:
        return normalize_deepswe_run(payload)
    return store.create_run(payload)


def normalize_deepswe_run(payload: Dict[str, Any]) -> Dict[str, Any]:
    context = normalize_deepswe_context(payload.get("pinned_context") or payload.get("context") or {})
    task_results = parse_deepswe_task_results(payload.get("task_results") or [])
    summary = payload.get("summary") or summarize_deepswe_results(task_results, context, infrastructure_failure=payload.get("infrastructure_failure") or {})
    return {
        "object": "hermes.dev_deepswe_benchmark_run",
        "run_id": str(payload.get("run_id") or "").strip(),
        "status": str(payload.get("status") or "completed").strip().lower(),
        "mode": _normalize_mode(payload.get("mode")),
        "provider": "deepswe",
        "pinned_context": context,
        "task_results": task_results,
        "summary": compact_deepswe_evidence(summary),
        "command": compact_deepswe_evidence(payload.get("command") or {}),
        "infrastructure_failure": compact_deepswe_evidence(payload.get("infrastructure_failure") or {}),
        "artifact_refs": compact_deepswe_evidence(payload.get("artifact_refs") or []),
        "blockers": compact_deepswe_evidence(payload.get("blockers") or context.get("blockers") or []),
        "created_at": float(payload.get("created_at") or time.time()),
        "updated_at": float(payload.get("updated_at") or time.time()),
        "completed_at": payload.get("completed_at"),
    }


def normalize_deepswe_context(context: Dict[str, Any]) -> Dict[str, Any]:
    task_ids = _normalize_list(context.get("task_ids") or context.get("tasks"))
    subset_seed = context.get("subset_seed") if context.get("subset_seed") is not None else context.get("sample_seed")
    n_tasks = context.get("n_tasks")
    resource_limits = compact_deepswe_evidence(context.get("resource_limits") or {})
    if not resource_limits:
        resource_limits = {
            "timeout_seconds": _int_or_none(context.get("timeout_seconds")) or 3600,
            "max_cost_usd": _float_or_none(context.get("max_cost_usd")),
            "max_tasks": _int_or_none(n_tasks) or len(task_ids) or None,
        }
    network_policy = compact_deepswe_evidence(context.get("network_policy") or {"mode": "agent_allowlist"})
    artifact_refs = compact_deepswe_evidence(context.get("artifact_refs") or [])
    normalized = {
        "deepswe_repo_url": _first_str(context.get("deepswe_repo_url"), context.get("repo_url"), "https://github.com/datacurve-ai/deep-swe"),
        "deepswe_commit": _first_str(context.get("deepswe_commit"), context.get("commit")),
        "deepswe_checkout_path": _first_str(context.get("deepswe_checkout_path"), context.get("checkout_path")),
        "pier_executable": _first_str(context.get("pier_executable"), context.get("pier_path"), "pier"),
        "pier_version": _first_str(context.get("pier_version")),
        "agent_adapter": _first_str(context.get("agent_adapter"), context.get("agent"), "mini-swe-agent"),
        "model_profile": _first_str(context.get("model_profile"), context.get("model")),
        "task_ids": task_ids,
        "subset_seed": subset_seed,
        "n_tasks": _int_or_none(n_tasks),
        "resource_limits": resource_limits,
        "network_policy": network_policy,
        "lab_environment_id": _first_str(context.get("lab_environment_id"), os.getenv("HERMES_LAB_ENVIRONMENT_ID"), "lab"),
        "artifact_dir": _first_str(context.get("artifact_dir")),
        "artifact_refs": artifact_refs,
        "scoring_rubric": _first_str(context.get("scoring_rubric"), "deepswe_verifier_pass_rate"),
        "environment_fingerprint": _first_str(context.get("environment_fingerprint")),
        "exploratory": bool(context.get("exploratory")),
    }
    normalized["task_subset_hash"] = task_subset_hash(normalized)
    blockers = _context_blockers(normalized)
    if blockers:
        normalized["blockers"] = blockers
    return compact_deepswe_evidence(normalized)


def build_pier_command(context: Dict[str, Any]) -> Dict[str, Any]:
    task_path = "tasks"
    checkout = context.get("deepswe_checkout_path")
    if checkout:
        task_path = str(Path(str(checkout)) / "tasks")
    task_ids = [str(task_id) for task_id in (context.get("task_ids") or []) if str(task_id or "").strip()]
    if checkout and len(task_ids) == 1:
        task_path = str(Path(str(checkout)) / "tasks" / task_ids[0])
    command = [
        str(context.get("pier_executable") or "pier"),
        "run",
        "-p",
        task_path,
        "--agent",
        str(context.get("agent_adapter") or "mini-swe-agent"),
    ]
    if context.get("model_profile"):
        command.extend(["--model", str(context["model_profile"])])
    if context.get("artifact_dir"):
        command.extend(["--jobs-dir", str(context["artifact_dir"])])
    resource_limits = context.get("resource_limits") or {}
    if resource_limits.get("n_concurrent"):
        command.extend(["--n-concurrent", str(resource_limits["n_concurrent"])])
    if len(task_ids) > 1:
        for task_id in task_ids:
            command.extend(["--include-task-name", task_id])
    elif context.get("n_tasks"):
        command.extend(["--n-tasks", str(context["n_tasks"])])
        if context.get("subset_seed") is not None:
            command.extend(["--sample-seed", str(context["subset_seed"])])
    return {
        "argv": command,
        "advisory_only": True,
        "side_effects": _no_side_effects(),
    }


def parse_deepswe_result_artifact(artifact: Any) -> Dict[str, Any]:
    if not artifact:
        return {"task_results": [], "artifact_refs": []}
    if isinstance(artifact, dict):
        return {
            "task_results": parse_deepswe_task_results(artifact.get("task_results") or artifact.get("results") or artifact.get("trials") or []),
            "artifact_refs": compact_deepswe_evidence(artifact.get("artifact_refs") or []),
        }
    path = Path(str(artifact)).expanduser()
    if not path.exists():
        return {"task_results": [], "artifact_refs": [{"path": str(path), "missing": True}]}
    if path.is_dir():
        results: list[Dict[str, Any]] = []
        refs = []
        for candidate in sorted(path.rglob("result.json")):
            parsed = parse_deepswe_result_artifact(candidate)
            results.extend(parsed.get("task_results") or [])
            refs.append({"path": str(candidate), "sha256": _file_sha256(candidate)})
        return {"task_results": results, "artifact_refs": refs}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    refs = [{"path": str(path), "sha256": _file_sha256(path)}]
    if isinstance(payload, list):
        return {"task_results": parse_deepswe_task_results(payload), "artifact_refs": refs}
    pier_trial = _pier_trial_task_result(payload, path)
    if pier_trial:
        return {"task_results": parse_deepswe_task_results([pier_trial]), "artifact_refs": refs}
    return {
        "task_results": parse_deepswe_task_results(payload.get("task_results") or payload.get("results") or payload.get("trials") or []),
        "artifact_refs": compact_deepswe_evidence(payload.get("artifact_refs") or refs),
    }


def _pier_trial_task_result(payload: Dict[str, Any], path: Path) -> Optional[Dict[str, Any]]:
    if not payload.get("task_name") and not payload.get("verifier_result") and not payload.get("exception_info"):
        return None
    rewards = ((payload.get("verifier_result") or {}).get("rewards") or {}) if isinstance(payload.get("verifier_result"), dict) else {}
    reward = _first_number(rewards.get("reward"))
    exception_info = payload.get("exception_info") if isinstance(payload.get("exception_info"), dict) else {}
    exception_text = " ".join(
        str(part)
        for part in (exception_info.get("exception_type"), exception_info.get("exception_message"))
        if str(part or "").strip()
    )
    status = "passed" if reward is not None and reward >= 1.0 and not exception_info else ("error" if exception_info else "failed")
    failure_category = _pier_failure_category(exception_text) if status != "passed" else None
    return {
        "task_id": payload.get("task_name") or (payload.get("task_id") or {}).get("path") or path.parent.name,
        "status": status,
        "verifier_status": "passed" if reward is not None and reward >= 1.0 else ("error" if exception_info else "failed"),
        "failure_category": failure_category,
        "message": _compact_pier_failure_message(failure_category, exception_info),
        "cost_usd": (payload.get("agent_result") or {}).get("cost_usd") if isinstance(payload.get("agent_result"), dict) else None,
        "input_tokens": (payload.get("agent_result") or {}).get("n_input_tokens") if isinstance(payload.get("agent_result"), dict) else None,
        "output_tokens": (payload.get("agent_result") or {}).get("n_output_tokens") if isinstance(payload.get("agent_result"), dict) else None,
        "artifact_ref": {"path": str(path), "sha256": _file_sha256(path)},
    }


def _pier_failure_category(text: Optional[str]) -> str:
    lowered = str(text or "").lower()
    if "api key" in lowered or "401 unauthorized" in lowered or "credential" in lowered:
        return "missing_credentials"
    if "timeout" in lowered:
        return "timeout"
    if "docker" in lowered or "image" in lowered or "environment" in lowered:
        return "dependency_environment_failure"
    return "verifier_failure"


def _compact_pier_failure_message(category: Optional[str], exception_info: Dict[str, Any]) -> str:
    exception_type = str(exception_info.get("exception_type") or "").strip()
    if category == "missing_credentials":
        return f"{exception_type or 'agent_error'}: missing or invalid agent credentials."
    if category == "timeout":
        return f"{exception_type or 'agent_error'}: agent or verifier timed out."
    if category == "dependency_environment_failure":
        return f"{exception_type or 'agent_error'}: benchmark environment failed."
    if exception_type:
        return f"{exception_type}: benchmark task failed; raw Pier message retained only in local artifact."
    return "Benchmark task failed; raw Pier message retained only in local artifact."


def parse_deepswe_task_results(results: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    parsed: list[Dict[str, Any]] = []
    for index, item in enumerate(results or []):
        if not isinstance(item, dict):
            continue
        status = _normalize_task_status(item)
        task_id = _first_str(item.get("task_id"), item.get("id"), item.get("name"), f"task-{index + 1}")
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        category = _first_str(item.get("failure_category"), metrics.get("failure_category"))
        if status != "passed":
            category = classify_deepswe_failure({**item, "status": status, "failure_category": category})
        parsed.append(compact_deepswe_evidence({
            "task_id": task_id,
            "status": status,
            "passed": status == "passed",
            "verifier_status": _first_str(item.get("verifier_status"), item.get("verifier"), status),
            "failure_category": category,
            "affected_component": _affected_component(category, item),
            "cost_usd": _first_number(item.get("cost_usd"), metrics.get("cost_usd"), item.get("cost")),
            "duration_seconds": _first_number(item.get("duration_seconds"), metrics.get("duration_seconds"), item.get("duration")),
            "input_tokens": _first_number(item.get("input_tokens"), metrics.get("input_tokens")),
            "output_tokens": _first_number(item.get("output_tokens"), metrics.get("output_tokens")),
            "peak_context_tokens": _first_number(item.get("peak_context_tokens"), metrics.get("peak_context_tokens")),
            "artifact_ref": item.get("artifact_ref") or item.get("trajectory_ref"),
            "error_type": item.get("error_type"),
            "message": _safe_message(item.get("message") or item.get("error")),
        }))
    return parsed


def summarize_deepswe_results(
    task_results: list[Dict[str, Any]],
    context: Dict[str, Any],
    *,
    infrastructure_failure: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    total = len(task_results)
    passed = sum(1 for item in task_results if item.get("status") == "passed")
    failed = sum(1 for item in task_results if item.get("status") == "failed")
    errored = sum(1 for item in task_results if item.get("status") == "error")
    timed_out = sum(1 for item in task_results if item.get("failure_category") == "timeout")
    verifier_errors = sum(1 for item in task_results if item.get("failure_category") in {"verifier_failure", "verifier_infrastructure_failure"})
    infra = sum(1 for item in task_results if item.get("failure_category") in INFRASTRUCTURE_FAILURE_CATEGORIES)
    return compact_deepswe_evidence({
        "provider": "deepswe",
        "sample_count": total,
        "task_count": total,
        "pass_count": passed,
        "failure_count": failed,
        "error_count": errored,
        "timeout_count": timed_out,
        "pass_rate": round(passed / total, 6) if total else 0.0,
        "success_rate": round(passed / total, 6) if total else 0.0,
        "failure_rate": round((failed + errored) / total, 6) if total else 0.0,
        "verifier_error_rate": round(verifier_errors / total, 6) if total else 0.0,
        "timeout_rate": round(timed_out / total, 6) if total else 0.0,
        "infrastructure_failure_rate": round(infra / total, 6) if total else (1.0 if infrastructure_failure else 0.0),
        "cost_usd": round(sum(float(item.get("cost_usd") or 0.0) for item in task_results), 6),
        "duration_seconds": round(sum(float(item.get("duration_seconds") or 0.0) for item in task_results), 6),
        "input_tokens": int(sum(float(item.get("input_tokens") or 0.0) for item in task_results)),
        "output_tokens": int(sum(float(item.get("output_tokens") or 0.0) for item in task_results)),
        "task_subset_hash": context.get("task_subset_hash") or task_subset_hash(context),
        "environment_fingerprint": context.get("environment_fingerprint") or environment_fingerprint(context),
        "infrastructure_failure": infrastructure_failure or {},
    })


def evaluate_deepswe_comparability(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    *,
    max_cost_ratio: float = 1.25,
    max_time_ratio: float = 1.25,
    max_verifier_error_delta: float = 0.0,
) -> Dict[str, Any]:
    before = _extract_context_and_summary(baseline)
    after = _extract_context_and_summary(candidate)
    blockers: list[Dict[str, Any]] = []
    comparable_fields = [
        "deepswe_commit",
        "task_subset_hash",
        "pier_version",
        "agent_adapter",
        "model_profile",
        "network_policy",
        "scoring_rubric",
    ]
    for field in comparable_fields:
        if before["context"].get(field) != after["context"].get(field):
            blockers.append(_blocker("deepswe_not_comparable", f"DeepSWE field {field} differs.", field=field, severity="inconclusive"))
    if (after["summary"].get("infrastructure_failure_rate") or 0) >= 0.5:
        blockers.append(_blocker("deepswe_infrastructure_dominated", "DeepSWE candidate run is dominated by infrastructure failures.", field="infrastructure_failure_rate", severity="blocked"))
    verifier_delta = float(after["summary"].get("verifier_error_rate") or 0) - float(before["summary"].get("verifier_error_rate") or 0)
    if verifier_delta > max_verifier_error_delta:
        blockers.append(_blocker("deepswe_verifier_error_regressed", "DeepSWE verifier-error rate regressed.", field="verifier_error_rate", severity="regressed"))
    cost_ratio = _ratio(after["summary"].get("cost_usd"), before["summary"].get("cost_usd"))
    if cost_ratio is not None and cost_ratio > max_cost_ratio:
        blockers.append(_blocker("deepswe_budget_regressed", "DeepSWE cost budget regressed.", field="cost_usd", severity="regressed"))
    time_ratio = _ratio(after["summary"].get("duration_seconds"), before["summary"].get("duration_seconds"))
    if time_ratio is not None and time_ratio > max_time_ratio:
        blockers.append(_blocker("deepswe_budget_regressed", "DeepSWE duration budget regressed.", field="duration_seconds", severity="regressed"))
    pass_delta = round(float(after["summary"].get("pass_rate") or 0) - float(before["summary"].get("pass_rate") or 0), 6)
    return {
        "ok": not blockers,
        "status": _status_from_blockers(blockers),
        "blockers": blockers,
        "metric_deltas": {
            "pass_rate": {
                "metric": "pass_rate",
                "baseline": before["summary"].get("pass_rate") or 0,
                "candidate": after["summary"].get("pass_rate") or 0,
                "delta": pass_delta,
            },
            "verifier_error_rate": {"delta": round(verifier_delta, 6)},
            "cost_ratio": cost_ratio,
            "duration_ratio": time_ratio,
        },
    }


def diagnose_deepswe_run(*, store: DevDeepSWEBenchmarkStore, run_id: str) -> Dict[str, Any]:
    run = _require_run(store, run_id)
    patterns_by_key: dict[str, Dict[str, Any]] = {}
    for item in run.get("task_results") or []:
        if item.get("status") == "passed":
            continue
        category = classify_deepswe_failure(item)
        component = _affected_component(category, item)
        key = f"{category}:{component}"
        pattern = patterns_by_key.setdefault(key, {
            "pattern_id": f"pattern-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:10]}",
            "failure_category": category,
            "affected_component": component,
            "task_ids": [],
            "count": 0,
            "severity": "medium",
            "confidence": 0.5,
            "actionable": False,
            "infrastructure": category in INFRASTRUCTURE_FAILURE_CATEGORIES,
        })
        pattern["task_ids"].append(item.get("task_id"))
        pattern["count"] += 1
    patterns = []
    total = max(len(run.get("task_results") or []), 1)
    for pattern in patterns_by_key.values():
        count = int(pattern["count"])
        infrastructure = bool(pattern.get("infrastructure"))
        pattern["severity"] = "high" if count >= 2 or infrastructure else "medium"
        pattern["confidence"] = round(min(0.95, 0.45 + (count / total)), 3)
        pattern["actionable"] = count >= 2 or infrastructure
        pattern["plan_brief"] = _plan_brief_for_pattern(pattern)
        patterns.append(pattern)
    patterns.sort(key=lambda item: (item.get("actionable") is not True, -int(item.get("count") or 0), item.get("failure_category") or ""))
    diagnosis = {
        "run_id": run_id,
        "status": "completed",
        "patterns": patterns,
        "exploratory": not any(pattern.get("actionable") for pattern in patterns),
    }
    return store.create_diagnosis(diagnosis)


def generate_deepswe_actions(
    *,
    store: DevDeepSWEBenchmarkStore,
    diagnosis_id: str,
    signal_store: Any = None,
    lab_store: Any = None,
    create_backlog_proposals: bool = False,
    create_lab_candidates: bool = False,
) -> Dict[str, Any]:
    diagnosis = store.get_diagnosis(diagnosis_id)
    if diagnosis is None:
        raise KeyError(f"DeepSWE diagnosis not found: {diagnosis_id}")
    created = []
    skipped = []
    for pattern in diagnosis.get("patterns") or []:
        if not pattern.get("actionable") or float(pattern.get("confidence") or 0) < 0.6:
            skipped.append({"pattern_id": pattern.get("pattern_id"), "reason": "exploratory_or_low_confidence"})
            continue
        action_payload = _action_payload(diagnosis, pattern)
        proposal_id = None
        lab_candidate_id = None
        if create_backlog_proposals and signal_store is not None:
            proposal = _create_backlog_proposal(signal_store, diagnosis, pattern, action_payload)
            proposal_id = proposal.get("proposal_id")
        if create_lab_candidates and lab_store is not None:
            candidate = _create_lab_candidate(lab_store, diagnosis, pattern, action_payload)
            lab_candidate_id = candidate.get("candidate_id")
        action = store.create_action({
            "diagnosis_id": diagnosis_id,
            "run_id": diagnosis["run_id"],
            "action_type": "benchmark_improvement",
            "status": "proposed",
            "payload": action_payload,
            "proposal_id": proposal_id,
            "lab_candidate_id": lab_candidate_id,
        })
        created.append(action)
    return {
        "ok": True,
        "object": "hermes.dev_deepswe_action_generation",
        "diagnosis_id": diagnosis_id,
        "run_id": diagnosis["run_id"],
        "created": created,
        "skipped": skipped,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "advisory_only": True,
        "side_effects": _no_side_effects(),
    }


def deepswe_promotion_evidence(run: Dict[str, Any], *, role: str = "candidate") -> Dict[str, Any]:
    summary = run.get("summary") or {}
    context = run.get("pinned_context") or {}
    return compact_deepswe_evidence({
        "provider": "deepswe",
        "source": "deepswe_lab_benchmark",
        "role": role,
        "run_id": run.get("run_id"),
        "deepswe_run_ids": [run.get("run_id")] if run.get("run_id") else [],
        "task_subset_hash": context.get("task_subset_hash"),
        "pinned_context": context,
        "summary": summary,
        "metrics": {
            "pass_rate": summary.get("pass_rate"),
            "success_rate": summary.get("success_rate"),
            "failure_rate": summary.get("failure_rate"),
            "verifier_error_rate": summary.get("verifier_error_rate"),
            "cost_usd": summary.get("cost_usd"),
            "duration_seconds": summary.get("duration_seconds"),
        },
        "sample_count": summary.get("sample_count"),
        "infrastructure_failure": run.get("infrastructure_failure") or {},
    })


def classify_deepswe_failure(task_result: Dict[str, Any]) -> str:
    explicit = str(task_result.get("failure_category") or "").strip().lower()
    if explicit in FAILURE_CATEGORIES or explicit in INFRASTRUCTURE_FAILURE_CATEGORIES:
        return explicit
    text = " ".join(str(task_result.get(key) or "").lower() for key in ("message", "error", "error_type", "verifier_status", "status"))
    if "timeout" in text or task_result.get("status") == "timeout":
        return "timeout"
    if "api key" in text or "401 unauthorized" in text or "credential" in text:
        return "missing_credentials"
    if "dependency" in text or "image" in text or "environment" in text:
        return "dependency_environment_failure"
    if "wrong file" in text or "unrelated file" in text:
        return "wrong_file_edit"
    if "incomplete" in text or "partial" in text or "missing requirement" in text:
        return "incomplete_implementation"
    if "contract" in text or "format" in text or "marker" in text:
        return "output_contract_failure"
    if "regression" in text or "breaks existing" in text:
        return "regression"
    if "cost" in text or "budget" in text:
        return "cost_blowout"
    if "navigation" in text or "could not find" in text or "no such file" in text:
        return "navigation_failure"
    return "verifier_failure"


def task_subset_hash(context: Dict[str, Any]) -> str:
    payload = {
        "task_ids": sorted(_normalize_list(context.get("task_ids"))),
        "subset_seed": context.get("subset_seed"),
        "n_tasks": context.get("n_tasks"),
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()[:16]


def environment_fingerprint(context: Dict[str, Any]) -> str:
    payload = {
        "pier_version": context.get("pier_version"),
        "agent_adapter": context.get("agent_adapter"),
        "model_profile": context.get("model_profile"),
        "resource_limits": context.get("resource_limits"),
        "network_policy": context.get("network_policy"),
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()[:16]


def compact_deepswe_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        return "<truncated>"
    if isinstance(value, dict):
        compact: Dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text in RAW_BENCHMARK_KEYS:
                compact[key_text] = "<omitted>"
                continue
            compact[key_text] = compact_deepswe_evidence(item, depth=depth + 1)
        return compact
    if isinstance(value, list):
        return [compact_deepswe_evidence(item, depth=depth + 1) for item in value[:100]]
    if isinstance(value, str) and len(value) > 1000:
        return value[:1000] + "...<truncated>"
    return value


def _validate_real_deepswe_run(context: Dict[str, Any]) -> None:
    checkout = Path(str(context.get("deepswe_checkout_path") or "")).expanduser()
    if not checkout.exists():
        raise FileNotFoundError(f"DeepSWE checkout not found: {checkout}")
    pier = str(context.get("pier_executable") or "pier")
    if Path(pier).is_absolute() and not Path(pier).exists():
        raise FileNotFoundError(f"Pier executable not found: {pier}")
    if not Path(pier).is_absolute() and shutil.which(pier) is None:
        raise FileNotFoundError(f"Pier executable not found on PATH: {pier}")
    paths = lab_paths_from_env()
    validate_lab_or_raise(
        hermes_home=Path(DEFAULT_DB_PATH).expanduser().parent if DEFAULT_DB_PATH else Path.home() / ".oryn-lab",
        gateway_port=os.getenv("API_SERVER_PORT") or 8662,
        repo_roots=[checkout, Path(paths["repos_dir"])],
    )


def _execute_pier_command(command: Dict[str, Any], *, cwd: Path, timeout_seconds: float, runner: Any = None) -> Dict[str, Any]:
    argv = command.get("argv") or []
    if runner is not None:
        return runner(argv=argv, cwd=cwd, timeout_seconds=timeout_seconds)
    try:
        completed = subprocess.run(
            argv,
            cwd=str(cwd),
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"DeepSWE Pier command timed out after {timeout_seconds}s") from exc
    failure = {}
    if completed.returncode != 0:
        failure = _infra_failure("pier_failure", completed.stderr or completed.stdout or f"pier exited {completed.returncode}")
    return {
        "returncode": completed.returncode,
        "infrastructure_failure": failure,
        "artifact_refs": [],
    }


def _create_backlog_proposal(signal_store: Any, diagnosis: Dict[str, Any], pattern: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    proposal = {
        "proposal_id": f"devprop-{uuid.uuid4().hex[:10]}",
        "report_id": None,
        "cluster_key": f"deepswe:{pattern.get('pattern_id')}",
        "status": "proposed",
        "payload": {
            "source": "deepswe",
            "title": payload["title"],
            "suggested_change": payload["plan_brief"],
            "target_category": pattern.get("affected_component"),
            "guardrail_touching": False,
            "status": "proposed",
        },
        "evidence_refs": payload["evidence_refs"],
        "query_descriptor": {
            "source": "deepswe",
            "diagnosis_id": diagnosis.get("diagnosis_id"),
            "failure_category": pattern.get("failure_category"),
        },
        "source_window": {"start": now, "end": now, "days": 0, "count": pattern.get("count") or 0, "rate_per_day": 0},
        "seeded_clarification_id": None,
        "linked_plan_id": None,
        "outcome": {},
        "created_at": now,
        "updated_at": now,
        "reviewed_at": None,
        "promoted_at": None,
        "measured_at": None,
    }
    return signal_store.create_proposal(proposal)


def _create_lab_candidate(lab_store: Any, diagnosis: Dict[str, Any], pattern: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    candidate = {
        "candidate_id": f"dogfood:deepswe:{pattern.get('pattern_id')}",
        "prompt": payload["plan_brief"],
        "profile_id": "platform.implement",
        "risk_level": "low",
        "target_paths": ["gateway/dev_control/", "tests/gateway/"],
        "source": "deepswe",
        "approved": False,
        "payload": {
            "diagnosis_id": diagnosis.get("diagnosis_id"),
            "run_id": diagnosis.get("run_id"),
            "failure_category": pattern.get("failure_category"),
            "evidence_refs": payload["evidence_refs"],
        },
    }
    return lab_store.upsert_candidate(candidate, approved=False)


def _action_payload(diagnosis: Dict[str, Any], pattern: Dict[str, Any]) -> Dict[str, Any]:
    return compact_deepswe_evidence({
        "title": f"Address DeepSWE {pattern.get('failure_category')} pattern",
        "source": "deepswe",
        "run_id": diagnosis.get("run_id"),
        "diagnosis_id": diagnosis.get("diagnosis_id"),
        "pattern_id": pattern.get("pattern_id"),
        "failure_category": pattern.get("failure_category"),
        "affected_component": pattern.get("affected_component"),
        "task_ids": pattern.get("task_ids") or [],
        "plan_brief": pattern.get("plan_brief") or _plan_brief_for_pattern(pattern),
        "expected_benchmark_movement": "Improve DeepSWE pass rate or reduce this failure category in the same pinned subset.",
        "verification_approach": "Re-run the same pinned DeepSWE subset and the focused Dev harness tests.",
        "non_goals": [
            "Do not tune directly against raw DeepSWE prompts.",
            "Do not auto-merge, release, publish, or change runtime policy.",
        ],
        "rollback_notes": "Dismiss the proposal or revert the harness change if comparable DeepSWE or Stable confirmation regresses.",
        "evidence_refs": [
            {"type": "deepswe_run", "id": diagnosis.get("run_id")},
            {"type": "deepswe_diagnosis", "id": diagnosis.get("diagnosis_id")},
            {"type": "deepswe_pattern", "id": pattern.get("pattern_id")},
        ],
    })


def _plan_brief_for_pattern(pattern: Dict[str, Any]) -> str:
    category = pattern.get("failure_category")
    component = pattern.get("affected_component") or "dev_harness"
    if category in INFRASTRUCTURE_FAILURE_CATEGORIES:
        return f"Investigate DeepSWE benchmark infrastructure for {category}; fix Pier/task environment setup before judging harness quality."
    if category == "navigation_failure":
        return "Improve repository navigation and grounding before edits; add benchmark-guided checks for locating relevant files."
    if category == "wrong_file_edit":
        return "Tighten edit targeting and diff review so workers avoid unrelated files on long-horizon tasks."
    if category == "incomplete_implementation":
        return "Improve planning and completion checks for multi-file DeepSWE tasks before verifier execution."
    if category == "timeout":
        return "Improve long-horizon task budgeting, progress checks, and timeout recovery in the Dev harness."
    if category == "cost_blowout":
        return "Add cost guardrails and early-stop criteria for expensive long-horizon benchmark attempts."
    return f"Plan a harness improvement for repeated DeepSWE {category} failures in {component}."


def _affected_component(category: Optional[str], item: Dict[str, Any]) -> str:
    explicit = _first_str(item.get("affected_component"), item.get("component"))
    if explicit:
        return explicit
    mapping = {
        "navigation_failure": "repo_grounding",
        "wrong_file_edit": "diff_scope",
        "incomplete_implementation": "execution_supervision",
        "output_contract_failure": "worker_output_contract",
        "timeout": "runtime_budgeting",
        "dependency_environment_failure": "benchmark_infrastructure",
        "verifier_failure": "verification",
        "regression": "guardrails",
        "cost_blowout": "runtime_budgeting",
    }
    return mapping.get(str(category or ""), "dev_harness")


def _context_blockers(context: Dict[str, Any]) -> list[Dict[str, Any]]:
    blockers = []
    for field in sorted(REQUIRED_CONTEXT_FIELDS):
        if not context.get(field):
            blockers.append(_blocker("missing_pinned_context", f"DeepSWE run context is missing {field}.", field=field, severity="blocked"))
    if not context.get("task_ids") and context.get("n_tasks") is None:
        blockers.append(_blocker("missing_task_subset", "DeepSWE run requires explicit task IDs or n_tasks/subset seed.", field="task_ids", severity="blocked"))
    if not context.get("resource_limits"):
        blockers.append(_blocker("missing_resource_limits", "DeepSWE run requires resource limits.", field="resource_limits", severity="blocked"))
    return blockers


def _extract_context_and_summary(value: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if value.get("pinned_context") or value.get("summary"):
        return {"context": value.get("pinned_context") or {}, "summary": value.get("summary") or {}}
    if value.get("provider") == "deepswe":
        return {"context": value.get("pinned_context") or value.get("context") or {}, "summary": value.get("summary") or value.get("metrics") or {}}
    return {"context": value.get("context") or {}, "summary": value.get("metrics") or value}


def _run_values(run: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        run["run_id"],
        run["status"],
        run["mode"],
        run["provider"],
        _json(run.get("pinned_context") or {}),
        _json(run.get("task_results") or []),
        _json(run.get("summary") or {}),
        _json(run.get("command") or {}),
        _json(run.get("infrastructure_failure") or {}),
        _json(run.get("artifact_refs") or []),
        _json(run.get("blockers") or []),
        float(run["created_at"]),
        float(run["updated_at"]),
        run.get("completed_at"),
        _json(run),
    )


def _run_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "object": "hermes.dev_deepswe_benchmark_run",
        "run_id": row["run_id"],
        "status": row["status"],
        "mode": row["mode"],
        "provider": row["provider"],
        "pinned_context": _loads(row["pinned_context"], {}),
        "task_results": _loads(row["task_results"], []),
        "summary": _loads(row["summary"], {}),
        "command": _loads(row["command"], {}),
        "infrastructure_failure": _loads(row["infrastructure_failure"], {}),
        "artifact_refs": _loads(row["artifact_refs"], []),
        "blockers": _loads(row["blockers"], []),
        "created_at": float(row["created_at"]),
        "updated_at": float(row["updated_at"]),
        "completed_at": row["completed_at"],
    }


def _run_summary_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    payload = _run_from_row(row)
    return {
        "object": "hermes.dev_deepswe_benchmark_run_summary",
        "run_id": payload["run_id"],
        "status": payload["status"],
        "mode": payload["mode"],
        "provider": payload["provider"],
        "created_at": payload["created_at"],
        "completed_at": payload["completed_at"],
        "summary": payload.get("summary") or {},
        "pinned_context": {
            key: (payload.get("pinned_context") or {}).get(key)
            for key in ("deepswe_commit", "pier_version", "agent_adapter", "model_profile", "task_subset_hash")
        },
    }


def _diagnosis_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "object": "hermes.dev_deepswe_diagnosis",
        "diagnosis_id": row["diagnosis_id"],
        "run_id": row["run_id"],
        "status": row["status"],
        "patterns": _loads(row["patterns"], []),
        "exploratory": bool(row["exploratory"]),
        "created_at": float(row["created_at"]),
    }


def _action_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "object": "hermes.dev_deepswe_action",
        "action_id": row["action_id"],
        "diagnosis_id": row["diagnosis_id"],
        "run_id": row["run_id"],
        "status": row["status"],
        "action_type": row["action_type"],
        "payload": _loads(row["payload"], {}),
        "proposal_id": row["proposal_id"],
        "lab_candidate_id": row["lab_candidate_id"],
        "created_at": float(row["created_at"]),
        "updated_at": float(row["updated_at"]),
    }


def _require_run(store: DevDeepSWEBenchmarkStore, run_id: str) -> Dict[str, Any]:
    run = store.get_run(run_id)
    if run is None:
        raise KeyError(f"DeepSWE benchmark run not found: {run_id}")
    return run


def _normalize_mode(value: Any) -> str:
    mode = str(value or "fixture").strip().lower().replace("-", "_")
    return mode if mode in {"fixture", "dry_run", "live", "pilot", "pilot_preflight"} else "fixture"


def _normalize_task_status(item: Dict[str, Any]) -> str:
    raw = str(item.get("status") or item.get("outcome") or "").strip().lower()
    if item.get("passed") is True or raw in {"passed", "pass", "success", "succeeded"}:
        return "passed"
    if raw in {"error", "errored", "infra_error"} or item.get("error"):
        return "error"
    if raw in {"timeout", "timed_out"}:
        return "error"
    return "failed"


def _normalize_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if value is None:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _blocker(code: str, message: str, *, field: Optional[str] = None, severity: str = "blocked") -> Dict[str, Any]:
    payload = {"code": code, "message": message, "severity": severity}
    if field:
        payload["field"] = field
    return payload


def _status_from_blockers(blockers: list[Dict[str, Any]]) -> str:
    severities = {str(blocker.get("severity") or "") for blocker in blockers}
    if "regressed" in severities:
        return "regressed"
    if "blocked" in severities:
        return "blocked"
    if "inconclusive" in severities:
        return "inconclusive"
    return "qualified"


def _infra_failure(category: str, message: str) -> Dict[str, Any]:
    return {"category": category, "message": _safe_message(message), "infrastructure": True}


def _no_side_effects() -> Dict[str, bool]:
    return {
        "merged": False,
        "released": False,
        "published": False,
        "branch_protection_changed": False,
        "service_mutated": False,
        "leaderboard_submitted": False,
    }


def _safe_message(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text[:500] + "...<truncated>" if len(text) > 500 else text


def _first_str(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_number(*values: Any) -> Optional[float]:
    for value in values:
        number = _float_or_none(value)
        if number is not None:
            return number
    return None


def _float_or_none(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ratio(candidate: Any, baseline: Any) -> Optional[float]:
    left = _float_or_none(candidate)
    right = _float_or_none(baseline)
    if left is None or right is None or right <= 0:
        return None
    return round(left / right, 6)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: Any, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return default
