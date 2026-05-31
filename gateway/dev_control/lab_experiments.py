"""Durable Lab experiment ledger for benchmark-backed harness decisions."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_lab_experiments (
    experiment_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    hypothesis TEXT NOT NULL,
    target_area TEXT NOT NULL,
    owner TEXT,
    scope TEXT NOT NULL,
    evidence_refs TEXT NOT NULL,
    decision TEXT NOT NULL,
    action_refs TEXT NOT NULL,
    blockers TEXT NOT NULL,
    rollback_plan TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_lab_experiments_status
    ON dev_lab_experiments(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_lab_experiments_target_area
    ON dev_lab_experiments(target_area, updated_at DESC);
"""

EXPERIMENT_STATUSES = {
    "draft",
    "collecting",
    "ready",
    "promote",
    "iterate",
    "reject",
    "inconclusive",
    "archived",
}

RAW_EVIDENCE_KEYS = {
    "corpus",
    "event_log",
    "events",
    "instruction",
    "messages",
    "prompt",
    "prompts",
    "raw_messages",
    "raw_transcript",
    "reference_solution",
    "solution",
    "stderr",
    "stdout",
    "test_patch",
    "trajectory",
    "trajectories",
    "transcript",
    "verifier_patch",
    "worker_messages",
}

COMPARABILITY_FIELDS = (
    "benchmark_commit",
    "task_subset_hash",
    "pier_version",
    "agent_adapter",
    "model_profile",
    "resource_limits_hash",
    "network_policy_hash",
    "scoring_rubric",
)

PRIMARY_METRICS = ("score", "pass_rate", "success_rate", "verification_success_rate")


@dataclass
class DevLabExperimentStore:
    """Persistence for Lab experiment records and compact evidence refs."""

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

    def create_experiment(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        experiment = normalize_lab_experiment({
            **payload,
            "experiment_id": payload.get("experiment_id") or f"devexp-{uuid.uuid4().hex[:10]}",
            "created_at": payload.get("created_at") or now,
            "updated_at": payload.get("updated_at") or now,
        })
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_lab_experiments (
                    experiment_id, status, hypothesis, target_area, owner, scope,
                    evidence_refs, decision, action_refs, blockers, rollback_plan,
                    created_at, updated_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _experiment_values(experiment),
            )
        return self.get_experiment(experiment["experiment_id"]) or experiment

    def update_experiment(self, experiment_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_experiment(experiment_id)
        if current is None:
            raise KeyError(f"Lab experiment not found: {experiment_id}")
        merged = normalize_lab_experiment({**current, **updates, "updated_at": time.time()})
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_lab_experiments
                SET status = ?, hypothesis = ?, target_area = ?, owner = ?, scope = ?,
                    evidence_refs = ?, decision = ?, action_refs = ?, blockers = ?,
                    rollback_plan = ?, created_at = ?, updated_at = ?, payload = ?
                WHERE experiment_id = ?
                """,
                (*_experiment_values(merged)[1:], experiment_id),
            )
        return self.get_experiment(experiment_id) or merged

    def get_experiment(self, experiment_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_lab_experiments WHERE experiment_id = ?",
            (str(experiment_id or "").strip(),),
        ).fetchone()
        return _experiment_from_row(row) if row else None

    def list_experiments(
        self,
        *,
        status: Optional[str] = None,
        target_area: Optional[str] = None,
        limit: int = 50,
    ) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(str(status).strip().lower())
        if target_area:
            clauses.append("target_area = ?")
            params.append(str(target_area).strip())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_lab_experiments
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_experiment_from_row(row) for row in rows]


def create_lab_experiment(*, store: DevLabExperimentStore, payload: Dict[str, Any]) -> Dict[str, Any]:
    return store.create_experiment(payload)


def attach_experiment_evidence(
    *,
    store: DevLabExperimentStore,
    experiment_id: str,
    evidence: Dict[str, Any] | list[Dict[str, Any]],
    stores: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    experiment = _require_experiment(store, experiment_id)
    incoming = evidence if isinstance(evidence, list) else [evidence]
    refs = list(experiment.get("evidence_refs") or [])
    blockers = list(experiment.get("blockers") or [])
    for item in incoming:
        ref = normalize_experiment_evidence(item)
        ref_blockers = _validate_evidence_ref(ref, stores or {})
        if ref_blockers:
            ref["validation_status"] = "missing"
            blockers.extend(ref_blockers)
        refs.append(ref)
    return store.update_experiment(experiment_id, {
        "status": "ready" if refs else experiment.get("status"),
        "evidence_refs": refs,
        "blockers": blockers,
    })


def evaluate_lab_experiment(
    *,
    store: DevLabExperimentStore,
    experiment_id: str,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    experiment = _require_experiment(store, experiment_id)
    decision = evaluate_experiment_decision(experiment, thresholds=thresholds)
    return store.update_experiment(experiment_id, {
        "status": decision["status"],
        "decision": decision,
        "blockers": decision.get("blockers") or [],
    })


def generate_experiment_actions(
    *,
    store: DevLabExperimentStore,
    experiment_id: str,
    signal_store: Any = None,
    lab_store: Any = None,
    create_backlog_proposals: bool = False,
    create_lab_candidates: bool = False,
) -> list[Dict[str, Any]]:
    experiment = _require_experiment(store, experiment_id)
    decision = experiment.get("decision") or evaluate_experiment_decision(experiment)
    if decision.get("status") not in {"iterate", "inconclusive"}:
        return []

    actions: list[Dict[str, Any]] = []
    patterns = decision.get("failure_patterns") or []
    if not patterns and decision.get("status") == "inconclusive":
        patterns = [{"failure_category": "experiment_evidence", "severity": "medium", "task_ids": []}]

    for pattern in patterns:
        action = _experiment_action(experiment, decision, pattern)
        if create_backlog_proposals and signal_store is not None:
            proposal = _create_backlog_proposal(signal_store, experiment, action)
            action["proposal_id"] = proposal.get("proposal_id")
        if create_lab_candidates and lab_store is not None:
            candidate = _create_lab_candidate(lab_store, experiment, action)
            action["lab_candidate_id"] = candidate.get("candidate_id")
            action["approved"] = bool(candidate.get("approved"))
        actions.append(action)

    merged_refs = list(experiment.get("action_refs") or [])
    merged_refs.extend(actions)
    store.update_experiment(experiment_id, {"action_refs": merged_refs})
    return actions


def experiment_promotion_reference(experiment: Dict[str, Any]) -> Dict[str, Any]:
    decision = experiment.get("decision") or {}
    return _compact_evidence({
        "experiment_id": experiment.get("experiment_id"),
        "status": experiment.get("status"),
        "decision_status": decision.get("status"),
        "hypothesis": experiment.get("hypothesis"),
        "target_area": experiment.get("target_area"),
        "evidence_refs": [
            {
                "role": ref.get("role"),
                "source": ref.get("source"),
                "run_id": ref.get("run_id") or ref.get("benchmark_run_id") or ref.get("lab_pass_id"),
                "task_subset_hash": ref.get("task_subset_hash"),
                "summary": ref.get("summary"),
            }
            for ref in (experiment.get("evidence_refs") or [])
            if isinstance(ref, dict)
        ],
        "metric_deltas": decision.get("metric_deltas"),
        "blockers": decision.get("blockers"),
        "action_refs": experiment.get("action_refs"),
        "stable_confirmation_required": True,
    })


def normalize_lab_experiment(payload: Dict[str, Any]) -> Dict[str, Any]:
    status = str(payload.get("status") or "draft").strip().lower()
    if status not in EXPERIMENT_STATUSES:
        status = "draft"
    scope = _compact_evidence(payload.get("scope") or payload.get("scope_metadata") or {})
    evidence_refs = [
        normalize_experiment_evidence(item)
        for item in (payload.get("evidence_refs") or payload.get("evidence") or [])
        if isinstance(item, dict)
    ]
    return {
        "object": "hermes.dev_lab_experiment",
        "experiment_id": str(payload.get("experiment_id") or "").strip(),
        "status": status,
        "hypothesis": _first_str(payload.get("hypothesis"), payload.get("title"), "Untitled Lab experiment") or "Untitled Lab experiment",
        "target_area": _first_str(payload.get("target_area"), payload.get("target_capability"), payload.get("component"), "dev_harness") or "dev_harness",
        "owner": _first_str(payload.get("owner"), payload.get("initiator")),
        "scope": scope,
        "evidence_refs": evidence_refs,
        "decision": _compact_evidence(payload.get("decision") or {}),
        "action_refs": _compact_evidence(payload.get("action_refs") or []),
        "blockers": _compact_evidence(payload.get("blockers") or []),
        "rollback_plan": _compact_evidence(payload.get("rollback_plan") or _default_rollback_plan(payload)),
        "created_at": float(payload.get("created_at") or time.time()),
        "updated_at": float(payload.get("updated_at") or time.time()),
    }


def normalize_experiment_evidence(evidence: Dict[str, Any]) -> Dict[str, Any]:
    source = _first_str(evidence.get("source"), evidence.get("type"), "benchmark") or "benchmark"
    role = str(evidence.get("role") or evidence.get("phase") or "").strip().lower()
    if role not in {"baseline", "candidate", "guardrail", "diagnosis", "action", "context"}:
        role = "candidate" if not evidence.get("baseline") else "baseline"
    summary = evidence.get("summary") if isinstance(evidence.get("summary"), dict) else {}
    metrics = evidence.get("metrics") if isinstance(evidence.get("metrics"), dict) else summary
    context = _normalize_evidence_context(evidence)
    return _compact_evidence({
        "evidence_id": _first_str(evidence.get("evidence_id"), evidence.get("id")) or f"evidence-{uuid.uuid4().hex[:8]}",
        "role": role,
        "source": source,
        "run_id": _first_str(evidence.get("run_id"), evidence.get("deepswe_run_id")),
        "benchmark_run_id": _first_str(evidence.get("benchmark_run_id")),
        "lab_pass_id": _first_str(evidence.get("lab_pass_id"), evidence.get("pass_id")),
        "diagnosis_id": _first_str(evidence.get("diagnosis_id")),
        "action_id": _first_str(evidence.get("action_id")),
        "task_subset_hash": _first_str(evidence.get("task_subset_hash"), context.get("task_subset_hash")),
        "artifact_refs": evidence.get("artifact_refs") or evidence.get("artifacts") or [],
        "summary": _summary_from_metrics(metrics),
        "context": context,
        "guardrails": evidence.get("guardrails") or evidence.get("gate_verdicts") or {},
        "failure_patterns": evidence.get("failure_patterns") or [],
        "validation_status": evidence.get("validation_status") or "unvalidated",
    })


def evaluate_experiment_decision(experiment: Dict[str, Any], *, thresholds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    thresholds = {
        "min_delta": 0.01,
        "min_sample_count": 2,
        "max_infrastructure_failure_rate": 0.25,
        **(thresholds or {}),
    }
    refs = [ref for ref in (experiment.get("evidence_refs") or []) if isinstance(ref, dict)]
    baseline = _first_role(refs, "baseline")
    candidate = _first_role(refs, "candidate")
    blockers: list[Dict[str, Any]] = []

    if baseline is None:
        blockers.append(_blocker("baseline_missing", "Experiment requires baseline evidence.", field="evidence_refs"))
    if candidate is None:
        blockers.append(_blocker("candidate_missing", "Experiment requires candidate evidence.", field="evidence_refs"))
    if baseline is None or candidate is None:
        return _decision("inconclusive", experiment, blockers=blockers, thresholds=thresholds)

    comparability = compare_experiment_evidence(baseline, candidate)
    if not comparability["ok"]:
        blockers.extend(comparability["blockers"])

    sample_count = _sample_count(candidate)
    if sample_count < int(thresholds["min_sample_count"]):
        blockers.append(_blocker(
            "sample_count_below_threshold",
            f"Candidate sample count {sample_count} is below required minimum {thresholds['min_sample_count']}.",
            field="candidate.summary.sample_count",
            severity="inconclusive",
            details={"sample_count": sample_count, "minimum": thresholds["min_sample_count"]},
        ))

    infra_rate = _number((candidate.get("summary") or {}).get("infrastructure_failure_rate")) or 0.0
    if infra_rate > float(thresholds["max_infrastructure_failure_rate"]):
        blockers.append(_blocker(
            "infrastructure_noise",
            "Candidate evidence is dominated by infrastructure failures.",
            field="candidate.summary.infrastructure_failure_rate",
            severity="inconclusive",
            details={"infrastructure_failure_rate": infra_rate},
        ))

    guardrails = candidate.get("guardrails") or {}
    if not guardrails:
        blockers.append(_blocker("guardrails_missing", "Experiment requires guardrail evidence.", field="candidate.guardrails", severity="inconclusive"))
    elif any(str(value).lower() in {"failed", "failure", "regressed", "blocked"} for value in guardrails.values()):
        blockers.append(_blocker("guardrail_regressed", "Candidate guardrail evidence regressed.", field="candidate.guardrails", severity="regressed"))

    metric_delta = _primary_delta(baseline, candidate)
    failure_patterns = _failure_patterns(candidate)
    if blockers:
        return _decision("inconclusive", experiment, blockers=blockers, comparability=comparability, metric_delta=metric_delta, failure_patterns=failure_patterns, thresholds=thresholds)
    if metric_delta["metric"] and metric_delta["delta"] >= float(thresholds["min_delta"]):
        return _decision("promote", experiment, comparability=comparability, metric_delta=metric_delta, failure_patterns=failure_patterns, thresholds=thresholds)
    if metric_delta["metric"] and metric_delta["delta"] < 0:
        return _decision("reject", experiment, comparability=comparability, metric_delta=metric_delta, failure_patterns=failure_patterns, thresholds=thresholds)
    if failure_patterns:
        return _decision("iterate", experiment, comparability=comparability, metric_delta=metric_delta, failure_patterns=failure_patterns, thresholds=thresholds)
    blockers.append(_blocker(
        "improvement_below_threshold",
        "Candidate evidence did not meet the configured improvement threshold.",
        field="candidate.summary",
        severity="inconclusive",
        details=metric_delta,
    ))
    return _decision("inconclusive", experiment, blockers=blockers, comparability=comparability, metric_delta=metric_delta, failure_patterns=failure_patterns, thresholds=thresholds)


def compare_experiment_evidence(baseline: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    blockers: list[Dict[str, Any]] = []
    baseline_context = baseline.get("context") or {}
    candidate_context = candidate.get("context") or {}
    for field in COMPARABILITY_FIELDS:
        left = baseline_context.get(field)
        right = candidate_context.get(field)
        if left is not None and right is not None and left != right:
            blockers.append(_blocker(
                "experiment_not_comparable",
                f"Experiment evidence differs for {field}.",
                field=f"context.{field}",
                severity="inconclusive",
                details={"baseline": left, "candidate": right},
            ))
    return {"ok": not blockers, "blockers": blockers, "checked_fields": list(COMPARABILITY_FIELDS)}


def _validate_evidence_ref(ref: Dict[str, Any], stores: Dict[str, Any]) -> list[Dict[str, Any]]:
    blockers: list[Dict[str, Any]] = []
    if ref.get("run_id") and stores.get("deepswe") is not None and stores["deepswe"].get_run(ref["run_id"]) is None:
        blockers.append(_blocker("deepswe_run_missing", "Referenced DeepSWE run was not found.", field="run_id"))
    if ref.get("lab_pass_id") and stores.get("lab") is not None and stores["lab"].get_pass(ref["lab_pass_id"]) is None:
        blockers.append(_blocker("lab_pass_missing", "Referenced Lab pass was not found.", field="lab_pass_id"))
    if ref.get("benchmark_run_id") and stores.get("benchmark") is not None and stores["benchmark"].get_run(ref["benchmark_run_id"]) is None:
        blockers.append(_blocker("benchmark_run_missing", "Referenced harness benchmark run was not found.", field="benchmark_run_id"))
    return blockers


def _experiment_action(experiment: Dict[str, Any], decision: Dict[str, Any], pattern: Dict[str, Any]) -> Dict[str, Any]:
    category = _first_str(pattern.get("failure_category"), pattern.get("category"), "experiment_evidence") or "experiment_evidence"
    return _compact_evidence({
        "action_id": f"devexp-act-{uuid.uuid4().hex[:10]}",
        "experiment_id": experiment.get("experiment_id"),
        "status": "proposed",
        "action_type": "lab_follow_up",
        "failure_category": category,
        "severity": pattern.get("severity") or "medium",
        "title": f"Address Lab experiment {category} finding",
        "plan_brief": f"Investigate {category} evidence from experiment {experiment.get('experiment_id')} and run a comparable follow-up benchmark.",
        "expected_benchmark_movement": "Reduce blockers or improve candidate metric in a comparable rerun.",
        "verification_approach": "Attach baseline and candidate evidence to a new or updated Lab experiment and re-evaluate.",
        "non_goals": [
            "Do not merge, publish, release, or mutate Stable from this action.",
            "Do not copy raw benchmark prompts, trajectories, or corpus data into durable records.",
        ],
        "rollback_notes": "Delete or supersede this advisory action; experiment evidence remains audit-only.",
        "decision_status": decision.get("status"),
        "evidence_refs": experiment_promotion_reference(experiment).get("evidence_refs"),
        "created_at": time.time(),
    })


def _create_backlog_proposal(signal_store: Any, experiment: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
    proposal = {
        "proposal_id": f"devprop-{uuid.uuid4().hex[:10]}",
        "report_id": None,
        "cluster_key": f"lab_experiment:{experiment.get('experiment_id')}:{action.get('failure_category')}",
        "status": "proposed",
        "payload": {
            "source": "lab_experiment",
            "title": action["title"],
            "suggested_change": action["plan_brief"],
            "target_category": experiment.get("target_area"),
            "guardrail_touching": False,
            "status": "proposed",
        },
        "evidence_refs": action.get("evidence_refs") or [],
        "query_descriptor": {
            "source": "lab_experiment",
            "experiment_id": experiment.get("experiment_id"),
            "action_id": action.get("action_id"),
        },
        "source_window": {},
        "outcome": {},
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    return signal_store.create_proposal(proposal)


def _create_lab_candidate(lab_store: Any, experiment: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
    candidate = {
        "candidate_id": f"dogfood:lab-exp-{uuid.uuid4().hex[:8]}",
        "prompt": action["plan_brief"],
        "profile_id": "dev-lab",
        "risk_level": "medium",
        "target_paths": ["gateway/dev_control/"],
        "source": "lab_experiment",
        "approved": False,
        "status": "candidate",
        "metadata": {
            "experiment_id": experiment.get("experiment_id"),
            "action_id": action.get("action_id"),
        },
    }
    return lab_store.upsert_candidate(candidate, approved=False)


def _decision(
    status: str,
    experiment: Dict[str, Any],
    *,
    blockers: Optional[list[Dict[str, Any]]] = None,
    comparability: Optional[Dict[str, Any]] = None,
    metric_delta: Optional[Dict[str, Any]] = None,
    failure_patterns: Optional[list[Dict[str, Any]]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return _compact_evidence({
        "object": "hermes.dev_lab_experiment_decision",
        "experiment_id": experiment.get("experiment_id"),
        "status": status,
        "promotable": status == "promote",
        "stable_confirmation_required": status == "promote",
        "comparability": comparability or {"ok": False, "blockers": []},
        "metric_deltas": {metric_delta["metric"]: metric_delta} if metric_delta and metric_delta.get("metric") else {},
        "failure_patterns": failure_patterns or [],
        "blockers": blockers or [],
        "thresholds": thresholds or {},
        "side_effects": _no_side_effects(),
        "evaluated_at": time.time(),
    })


def _normalize_evidence_context(evidence: Dict[str, Any]) -> Dict[str, Any]:
    context = evidence.get("context") if isinstance(evidence.get("context"), dict) else {}
    pinned = evidence.get("pinned_context") if isinstance(evidence.get("pinned_context"), dict) else {}
    merged = {**pinned, **context}
    resource_limits = merged.get("resource_limits") or evidence.get("resource_limits") or {}
    network_policy = merged.get("network_policy") or evidence.get("network_policy") or {}
    return _compact_evidence({
        "benchmark_commit": _first_str(merged.get("deepswe_commit"), merged.get("benchmark_commit"), merged.get("commit")),
        "task_subset_hash": _first_str(merged.get("task_subset_hash"), evidence.get("task_subset_hash")),
        "pier_version": _first_str(merged.get("pier_version"), evidence.get("pier_version")),
        "agent_adapter": _first_str(merged.get("agent_adapter"), evidence.get("agent_adapter")),
        "model_profile": _first_str(merged.get("model_profile"), merged.get("model"), evidence.get("model_profile")),
        "resource_limits_hash": _stable_hash(resource_limits) if resource_limits else None,
        "network_policy_hash": _stable_hash(network_policy) if network_policy else None,
        "scoring_rubric": _first_str(merged.get("scoring_rubric"), evidence.get("scoring_rubric")),
    })


def _summary_from_metrics(metrics: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: _number(metrics.get(key))
        for key in (
            "score",
            "pass_rate",
            "success_rate",
            "verification_success_rate",
            "failure_rate",
            "infrastructure_failure_rate",
            "sample_count",
            "task_count",
            "cost_usd",
            "duration_seconds",
        )
        if metrics.get(key) is not None and _number(metrics.get(key)) is not None
    }


def _primary_delta(baseline: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    base_summary = baseline.get("summary") or {}
    candidate_summary = candidate.get("summary") or {}
    for metric in PRIMARY_METRICS:
        left = _number(base_summary.get(metric))
        right = _number(candidate_summary.get(metric))
        if left is not None and right is not None:
            return {"metric": metric, "baseline": left, "candidate": right, "delta": round(right - left, 6)}
    return {"metric": None, "baseline": None, "candidate": None, "delta": 0.0}


def _failure_patterns(candidate: Dict[str, Any]) -> list[Dict[str, Any]]:
    patterns = candidate.get("failure_patterns") or []
    if not isinstance(patterns, list):
        return []
    return [_compact_evidence(item) for item in patterns if isinstance(item, dict)]


def _sample_count(evidence: Dict[str, Any]) -> int:
    summary = evidence.get("summary") or {}
    for key in ("sample_count", "task_count"):
        value = _number(summary.get(key))
        if value is not None:
            return int(value)
    return 0


def _first_role(refs: list[Dict[str, Any]], role: str) -> Optional[Dict[str, Any]]:
    return next((ref for ref in refs if ref.get("role") == role), None)


def _require_experiment(store: DevLabExperimentStore, experiment_id: str) -> Dict[str, Any]:
    experiment = store.get_experiment(experiment_id)
    if experiment is None:
        raise KeyError(f"Lab experiment not found: {experiment_id}")
    return experiment


def _experiment_values(experiment: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        experiment["experiment_id"],
        experiment["status"],
        experiment["hypothesis"],
        experiment["target_area"],
        experiment.get("owner"),
        _json(experiment.get("scope") or {}),
        _json(experiment.get("evidence_refs") or []),
        _json(experiment.get("decision") or {}),
        _json(experiment.get("action_refs") or []),
        _json(experiment.get("blockers") or []),
        _json(experiment.get("rollback_plan") or {}),
        float(experiment["created_at"]),
        float(experiment["updated_at"]),
        _json(experiment),
    )


def _experiment_from_row(row: sqlite3.Row | None) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    payload = _loads(row["payload"], {})
    payload.update({
        "experiment_id": row["experiment_id"],
        "status": row["status"],
        "hypothesis": row["hypothesis"],
        "target_area": row["target_area"],
        "owner": row["owner"],
        "scope": _loads(row["scope"], {}),
        "evidence_refs": _loads(row["evidence_refs"], []),
        "decision": _loads(row["decision"], {}),
        "action_refs": _loads(row["action_refs"], []),
        "blockers": _loads(row["blockers"], []),
        "rollback_plan": _loads(row["rollback_plan"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    })
    return payload


def _default_rollback_plan(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "rollback_order": [
            "Mark the experiment superseded or inconclusive.",
            "Revert any child harness implementation PR.",
            "Revert parent submodule pointer or docs updates if they landed.",
            "Keep compact experiment rows for audit.",
        ],
        "target_area": payload.get("target_area") or payload.get("target_capability") or "dev_harness",
    }


def _blocker(
    code: str,
    message: str,
    *,
    field: Optional[str] = None,
    severity: str = "blocked",
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return _compact_evidence({
        "code": code,
        "message": message,
        "field": field,
        "severity": severity,
        "details": details or {},
    })


def _no_side_effects() -> Dict[str, bool]:
    return {
        "merged": False,
        "released": False,
        "published": False,
        "branch_protection_changed": False,
        "service_mutated": False,
        "leaderboard_submitted": False,
    }


def _compact_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth > 6:
        return "<truncated>"
    if isinstance(value, dict):
        compact: Dict[str, Any] = {}
        for key, nested in value.items():
            key_str = str(key)
            if key_str in RAW_EVIDENCE_KEYS:
                compact[key_str] = "<omitted>"
            else:
                compact[key_str] = _compact_evidence(nested, depth=depth + 1)
        return compact
    if isinstance(value, list):
        return [_compact_evidence(item, depth=depth + 1) for item in value[:200]]
    return value


def _first_str(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _number(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _stable_hash(value: Any) -> str:
    import hashlib

    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()[:16]


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _loads(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return fallback
