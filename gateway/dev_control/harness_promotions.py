"""Lab-to-Stable promotion evidence for Dev harness improvements."""

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
CREATE TABLE IF NOT EXISTS dev_harness_promotions (
    promotion_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    environment TEXT NOT NULL,
    candidate_id TEXT,
    lab_pass_id TEXT,
    target_repo TEXT,
    target_capability TEXT,
    improvement_category TEXT NOT NULL,
    benchmark_run_ids TEXT NOT NULL,
    lab_evidence TEXT NOT NULL,
    benchmark_evidence TEXT NOT NULL,
    stable_evidence TEXT NOT NULL,
    qualification TEXT NOT NULL,
    package TEXT NOT NULL,
    blockers TEXT NOT NULL,
    pr_refs TEXT NOT NULL,
    merge_refs TEXT NOT NULL,
    rollback_plan TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_harness_promotions_status
    ON dev_harness_promotions(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_harness_promotions_lab_pass
    ON dev_harness_promotions(lab_pass_id, updated_at DESC);
"""

PROMOTION_STATUSES = {
    "candidate",
    "qualified",
    "inconclusive",
    "blocked",
    "regressed",
    "packaged",
    "pr_open",
    "merged",
    "stable_confirmed",
    "stable_rejected",
    "rolled_back",
}

RAW_EVIDENCE_KEYS = {
    "messages",
    "raw_messages",
    "raw_transcript",
    "transcript",
    "stdout",
    "stderr",
    "events",
    "event_log",
    "worker_messages",
    "prompt",
    "prompts",
    "instruction",
    "reference_solution",
    "solution",
    "verifier_patch",
    "test_patch",
    "trajectory",
    "trajectories",
}

THRESHOLD_BUNDLES: Dict[str, Dict[str, Any]] = {
    "output_contract": {
        "primary_metric": "score",
        "alternate_metrics": ["output_contract_score", "contract_score", "success_rate", "pass_rate"],
        "min_delta": 0.05,
        "min_sample_count": 5,
        "max_failure_rate_regression": 0.0,
        "max_success_rate_regression": 0.0,
        "max_cost_ratio": 1.20,
        "max_time_ratio": 1.20,
    },
    "runtime_policy": {
        "primary_metric": "success_rate",
        "alternate_metrics": ["pass_rate", "score", "output_contract_score"],
        "min_delta": 0.03,
        "min_sample_count": 20,
        "max_failure_rate_regression": 0.0,
        "max_success_rate_regression": 0.0,
        "max_cost_ratio": 1.10,
        "max_time_ratio": 1.10,
        "human_review_required": True,
    },
    "supervisor_policy": {
        "primary_metric": "success_rate",
        "alternate_metrics": ["pass_rate", "completion_rate", "score"],
        "min_delta": 0.04,
        "min_sample_count": 20,
        "max_failure_rate_regression": 0.0,
        "max_success_rate_regression": 0.0,
        "max_cost_ratio": 1.15,
        "max_time_ratio": 1.15,
        "human_review_required": True,
    },
    "verification": {
        "primary_metric": "verification_success_rate",
        "alternate_metrics": ["success_rate", "pass_rate", "score"],
        "min_delta": 0.02,
        "min_sample_count": 10,
        "max_failure_rate_regression": 0.0,
        "max_success_rate_regression": 0.0,
        "max_cost_ratio": 1.25,
        "max_time_ratio": 1.20,
    },
    "ci_review": {
        "primary_metric": "review_success_rate",
        "alternate_metrics": ["ci_success_rate", "success_rate", "pass_rate", "score"],
        "min_delta": 0.02,
        "min_sample_count": 10,
        "max_failure_rate_regression": 0.0,
        "max_success_rate_regression": 0.0,
        "max_cost_ratio": 1.25,
        "max_time_ratio": 1.20,
    },
}


@dataclass
class DevHarnessPromotionStore:
    """Persistence for Lab-to-Stable harness promotion records."""

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

    def create_promotion(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        promotion = normalize_promotion({
            **payload,
            "promotion_id": payload.get("promotion_id") or f"devprom-{uuid.uuid4().hex[:10]}",
            "status": payload.get("status") or "candidate",
            "environment": payload.get("environment") or "dev",
            "created_at": payload.get("created_at") or now,
            "updated_at": payload.get("updated_at") or now,
        })
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_harness_promotions (
                    promotion_id, status, environment, candidate_id, lab_pass_id,
                    target_repo, target_capability, improvement_category,
                    benchmark_run_ids, lab_evidence, benchmark_evidence, stable_evidence,
                    qualification, package, blockers, pr_refs, merge_refs, rollback_plan,
                    created_at, updated_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _promotion_values(promotion),
            )
        return self.get_promotion(promotion["promotion_id"]) or promotion

    def update_promotion(self, promotion_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_promotion(promotion_id)
        if current is None:
            raise KeyError(f"Dev harness promotion not found: {promotion_id}")
        merged = normalize_promotion({**current, **updates, "updated_at": time.time()})
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_harness_promotions
                SET status = ?, environment = ?, candidate_id = ?, lab_pass_id = ?,
                    target_repo = ?, target_capability = ?, improvement_category = ?,
                    benchmark_run_ids = ?, lab_evidence = ?, benchmark_evidence = ?,
                    stable_evidence = ?, qualification = ?, package = ?, blockers = ?,
                    pr_refs = ?, merge_refs = ?, rollback_plan = ?, created_at = ?,
                    updated_at = ?, payload = ?
                WHERE promotion_id = ?
                """,
                (*_promotion_values(merged)[1:], promotion_id),
            )
        return self.get_promotion(promotion_id) or merged

    def get_promotion(self, promotion_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_harness_promotions WHERE promotion_id = ?",
            (str(promotion_id or "").strip(),),
        ).fetchone()
        return _promotion_from_row(row) if row else None

    def list_promotions(
        self,
        *,
        status: Optional[str] = None,
        lab_pass_id: Optional[str] = None,
        limit: int = 50,
    ) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(str(status).strip())
        if lab_pass_id:
            clauses.append("lab_pass_id = ?")
            params.append(str(lab_pass_id).strip())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_harness_promotions
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_promotion_from_row(row) for row in rows]


def normalize_promotion(payload: Dict[str, Any]) -> Dict[str, Any]:
    lab_evidence = normalize_environment_evidence(payload.get("lab_evidence") or {}, environment="lab")
    benchmark_evidence = _compact_evidence(payload.get("benchmark_evidence") or {})
    stable_evidence = normalize_environment_evidence(payload.get("stable_evidence") or {}, environment="stable")
    benchmark_run_ids = _normalize_benchmark_ids(payload.get("benchmark_run_ids"), benchmark_evidence)
    status = str(payload.get("status") or "candidate").strip().lower()
    if status not in PROMOTION_STATUSES:
        status = "candidate"
    return {
        "object": "hermes.dev_harness_promotion",
        "promotion_id": str(payload.get("promotion_id") or "").strip(),
        "status": status,
        "environment": str(payload.get("environment") or "dev").strip().lower(),
        "candidate_id": _first_str(payload.get("candidate_id"), lab_evidence.get("candidate_id")),
        "lab_pass_id": _first_str(payload.get("lab_pass_id"), payload.get("pass_id"), lab_evidence.get("pass_id")),
        "target_repo": _first_str(payload.get("target_repo"), lab_evidence.get("target_repo")),
        "target_capability": _first_str(payload.get("target_capability"), lab_evidence.get("target_capability")),
        "improvement_category": _normalize_category(payload.get("improvement_category") or payload.get("category")),
        "benchmark_run_ids": benchmark_run_ids,
        "lab_evidence": lab_evidence,
        "benchmark_evidence": benchmark_evidence,
        "stable_evidence": stable_evidence,
        "qualification": _compact_evidence(payload.get("qualification") or {}),
        "package": _compact_evidence(payload.get("package") or {}),
        "blockers": _compact_evidence(payload.get("blockers") or []),
        "pr_refs": _compact_evidence(payload.get("pr_refs") or {}),
        "merge_refs": _compact_evidence(payload.get("merge_refs") or {}),
        "rollback_plan": _compact_evidence(payload.get("rollback_plan") or default_rollback_plan(payload)),
        "created_at": float(payload.get("created_at") or time.time()),
        "updated_at": float(payload.get("updated_at") or time.time()),
    }


def normalize_environment_evidence(evidence: Dict[str, Any], *, environment: str) -> Dict[str, Any]:
    normalized = _compact_evidence(evidence if isinstance(evidence, dict) else {})
    normalized["environment"] = environment
    normalized.setdefault("source", f"{environment}_promotion_evidence")
    return normalized


def create_promotion_candidate(
    *,
    store: DevHarnessPromotionStore,
    lab_evidence: Dict[str, Any],
    benchmark_evidence: Dict[str, Any],
    target_repo: Optional[str] = None,
    target_capability: Optional[str] = None,
    improvement_category: Optional[str] = None,
    benchmark_run_ids: Optional[list[str]] = None,
    qualify: bool = False,
) -> Dict[str, Any]:
    promotion = store.create_promotion({
        "candidate_id": lab_evidence.get("candidate_id"),
        "lab_pass_id": lab_evidence.get("pass_id") or lab_evidence.get("lab_pass_id"),
        "target_repo": target_repo or lab_evidence.get("target_repo"),
        "target_capability": target_capability or lab_evidence.get("target_capability"),
        "improvement_category": improvement_category or lab_evidence.get("improvement_category"),
        "benchmark_run_ids": benchmark_run_ids,
        "lab_evidence": lab_evidence,
        "benchmark_evidence": benchmark_evidence,
        "status": "candidate",
    })
    if qualify:
        return qualify_promotion(store=store, promotion_id=promotion["promotion_id"])
    return promotion


def qualify_promotion(*, store: DevHarnessPromotionStore, promotion_id: str) -> Dict[str, Any]:
    promotion = _require_promotion(store, promotion_id)
    qualification = evaluate_promotion_qualification(promotion)
    status = qualification["status"]
    return store.update_promotion(promotion_id, {
        "status": status,
        "qualification": qualification,
        "blockers": qualification.get("blockers") or [],
    })


def generate_promotion_package(*, store: DevHarnessPromotionStore, promotion_id: str) -> Dict[str, Any]:
    promotion = _require_promotion(store, promotion_id)
    if promotion.get("status") != "qualified":
        blockers = promotion.get("blockers") or (promotion.get("qualification") or {}).get("blockers") or []
        package = {
            "ok": False,
            "object": "hermes.dev_harness_promotion_package",
            "promotion_id": promotion_id,
            "blocked": True,
            "reason": "Promotion must be qualified before PR packaging.",
            "blockers": blockers,
            "side_effects": _no_side_effects(),
        }
        return store.update_promotion(promotion_id, {"package": package})

    package = _build_package(promotion)
    return store.update_promotion(promotion_id, {"status": "packaged", "package": package})


def record_promotion_pr(
    *,
    store: DevHarnessPromotionStore,
    promotion_id: str,
    pr_refs: Dict[str, Any],
) -> Dict[str, Any]:
    promotion = _require_promotion(store, promotion_id)
    refs = _compact_evidence({
        **(promotion.get("pr_refs") or {}),
        **(pr_refs or {}),
        "recorded_at": time.time(),
    })
    status = "pr_open" if promotion.get("status") in {"qualified", "packaged", "pr_open"} else promotion.get("status")
    return store.update_promotion(promotion_id, {"status": status, "pr_refs": refs})


def record_promotion_merge(
    *,
    store: DevHarnessPromotionStore,
    promotion_id: str,
    merge_refs: Dict[str, Any],
) -> Dict[str, Any]:
    promotion = _require_promotion(store, promotion_id)
    refs = _compact_evidence({
        **(promotion.get("merge_refs") or {}),
        **(merge_refs or {}),
        "recorded_at": time.time(),
    })
    return store.update_promotion(promotion_id, {"status": "merged", "merge_refs": refs})


def confirm_stable_promotion(
    *,
    store: DevHarnessPromotionStore,
    promotion_id: str,
    stable_evidence: Dict[str, Any],
) -> Dict[str, Any]:
    promotion = _require_promotion(store, promotion_id)
    normalized_stable = normalize_environment_evidence(stable_evidence or {}, environment="stable")
    confirmation = evaluate_stable_confirmation(promotion, normalized_stable)
    if confirmation["status"] == "qualified":
        status = "stable_confirmed"
    elif confirmation["status"] == "regressed":
        status = "regressed"
    else:
        status = "stable_rejected"
    normalized_stable["confirmation"] = confirmation
    return store.update_promotion(promotion_id, {
        "status": status,
        "stable_evidence": normalized_stable,
        "blockers": confirmation.get("blockers") or [],
    })


def evaluate_promotion_qualification(promotion: Dict[str, Any]) -> Dict[str, Any]:
    lab_evidence = promotion.get("lab_evidence") or {}
    benchmark_evidence = promotion.get("benchmark_evidence") or {}
    category = _normalize_category(promotion.get("improvement_category"))
    bundle = THRESHOLD_BUNDLES[category]
    blockers: list[Dict[str, Any]] = []
    deltas: Dict[str, Any] = {}

    _extend(blockers, _provenance_blockers(promotion, lab_evidence, benchmark_evidence))
    _extend(blockers, _lab_gate_blockers(lab_evidence))
    if _fixture_only(benchmark_evidence):
        blockers.append(_blocker(
            "fixture_only_evidence",
            "Fixture-only or dry-run-only evidence is exploratory and cannot qualify a Stable promotion by itself.",
            field="benchmark_evidence",
            severity="blocked",
        ))

    comparability = _comparability(benchmark_evidence)
    if not comparability["ok"]:
        _extend(blockers, comparability["blockers"])

    sample_count = _sample_count(benchmark_evidence)
    if sample_count < int(bundle["min_sample_count"]):
        blockers.append(_blocker(
            "sample_count_below_threshold",
            f"Benchmark sample count {sample_count} is below required minimum {bundle['min_sample_count']}.",
            field="benchmark_evidence.sample_count",
            severity="inconclusive",
            details={"sample_count": sample_count, "minimum": bundle["min_sample_count"]},
        ))

    primary = _metric_delta(benchmark_evidence, bundle)
    if primary["metric"] is None:
        blockers.append(_blocker(
            "primary_metric_missing",
            "Benchmark evidence is missing the category primary metric.",
            field="benchmark_evidence",
            severity="inconclusive",
        ))
    else:
        deltas[primary["metric"]] = primary
        if primary["delta"] < float(bundle["min_delta"]):
            blockers.append(_blocker(
                "improvement_below_threshold",
                f"Metric delta {primary['delta']:.4f} is below required {bundle['min_delta']:.4f}.",
                field=f"benchmark_evidence.{primary['metric']}",
                severity="inconclusive",
                details=primary,
            ))

    guardrails = _guardrail_blockers(benchmark_evidence, bundle)
    _extend(blockers, guardrails["blockers"])
    deltas.update(guardrails["deltas"])

    status = _qualification_status(blockers)
    return {
        "object": "hermes.dev_harness_promotion_qualification",
        "status": status,
        "qualified": status == "qualified",
        "category": category,
        "threshold_bundle": bundle,
        "sample_count": sample_count,
        "comparability": comparability,
        "metric_deltas": deltas,
        "blockers": blockers,
        "evaluated_at": time.time(),
    }


def evaluate_stable_confirmation(promotion: Dict[str, Any], stable_evidence: Dict[str, Any]) -> Dict[str, Any]:
    benchmark_evidence = stable_evidence.get("benchmark_evidence") or stable_evidence.get("benchmark") or stable_evidence
    synthetic = {
        **promotion,
        "benchmark_evidence": benchmark_evidence,
        "benchmark_run_ids": _normalize_benchmark_ids(stable_evidence.get("benchmark_run_ids"), benchmark_evidence),
        "lab_evidence": {
            "pass_id": promotion.get("lab_pass_id"),
            "candidate_id": promotion.get("candidate_id"),
            "draft_artifact": {"head_sha": (promotion.get("lab_evidence") or {}).get("head_sha") or "stable-confirmation"},
            "diff_scope": {"ok": True},
            "quarantined": False,
            "empty_diff": False,
        },
    }
    result = evaluate_promotion_qualification(synthetic)
    blockers = list(result.get("blockers") or [])
    if not promotion.get("merge_refs"):
        blockers.append(_blocker(
            "merge_refs_missing",
            "Stable confirmation requires recorded child or parent merge refs.",
            field="merge_refs",
            severity="blocked",
        ))
    if not synthetic["benchmark_run_ids"]:
        blockers.append(_blocker(
            "stable_benchmark_missing",
            "Stable confirmation requires Stable benchmark run IDs.",
            field="stable_evidence.benchmark_run_ids",
            severity="blocked",
        ))
    status = _qualification_status(blockers)
    return {
        **result,
        "object": "hermes.dev_harness_promotion_stable_confirmation",
        "status": status,
        "qualified": status == "qualified",
        "blockers": blockers,
        "stable_benchmark_run_ids": synthetic["benchmark_run_ids"],
    }


def default_rollback_plan(payload: Dict[str, Any]) -> Dict[str, Any]:
    target_repo = payload.get("target_repo") or "hermes-agent"
    return {
        "child_repo": target_repo,
        "rollback_order": [
            "Revert parent submodule pointer PR if one was merged.",
            "Revert or disable the child repo harness change.",
            "Keep promotion evidence rows for audit.",
        ],
    }


def _build_package(promotion: Dict[str, Any]) -> Dict[str, Any]:
    qualification = promotion.get("qualification") or {}
    lab = promotion.get("lab_evidence") or {}
    benchmark_ids = promotion.get("benchmark_run_ids") or []
    target_repo = promotion.get("target_repo") or "hermes-agent"
    touched_paths = lab.get("touched_paths") or []
    promotion_id = promotion["promotion_id"]
    title = f"Promote Lab harness improvement {promotion_id}"
    confirmation_command = (
        f"POST /v1/dev/harness/promotions/{promotion_id}/confirm-stable "
        "with comparable Stable benchmark evidence"
    )
    body = "\n".join([
        f"## Promotion {promotion_id}",
        "",
        f"- Lab pass: {promotion.get('lab_pass_id')}",
        f"- Lab candidate: {promotion.get('candidate_id')}",
        f"- Benchmark runs: {', '.join(benchmark_ids) if benchmark_ids else 'none'}",
        f"- Target repo: {target_repo}",
        f"- Target capability: {promotion.get('target_capability') or 'unspecified'}",
        f"- Improvement category: {promotion.get('improvement_category')}",
        f"- Draft artifact/head SHA: {_draft_ref(lab) or 'missing'}",
        f"- Touched paths: {', '.join(touched_paths) if touched_paths else 'none recorded'}",
        "",
        "## Qualification",
        "",
        f"- Status: {qualification.get('status')}",
        f"- Sample count: {qualification.get('sample_count')}",
        f"- Metric deltas: {_canonical_json(qualification.get('metric_deltas') or {})}",
        "",
        "## Gates",
        "",
        f"- Verification: {(lab.get('gate_verdicts') or {}).get('verification') or (lab.get('gates') or {}).get('verification') or 'unknown'}",
        f"- CI: {(lab.get('gate_verdicts') or {}).get('ci') or (lab.get('gates') or {}).get('ci') or 'unknown'}",
        f"- Review: {(lab.get('gate_verdicts') or {}).get('review') or (lab.get('gates') or {}).get('review') or 'unknown'}",
        "",
        "## Delivery",
        "",
        "- Merge order: child repo PR first, then parent Oryn submodule pointer PR if required.",
        "- Rollback order: parent pointer revert first, then child repo revert or feature disable.",
        f"- Stable confirmation: {confirmation_command}.",
        "- Stable confirmation has not run until separate Stable evidence is recorded.",
        *_deepswe_package_lines(promotion),
    ])
    return {
        "ok": True,
        "object": "hermes.dev_harness_promotion_package",
        "promotion_id": promotion_id,
        "title": title,
        "body": body,
        "evidence_ids": {
            "lab_pass_id": promotion.get("lab_pass_id"),
            "candidate_id": promotion.get("candidate_id"),
            "benchmark_run_ids": benchmark_ids,
        },
        "benchmark_deltas": qualification.get("metric_deltas") or {},
        "deepswe_evidence": _deepswe_package_summary(promotion),
        "gate_summary": lab.get("gate_verdicts") or lab.get("gates") or {},
        "touched_paths": touched_paths,
        "affected_repos": [target_repo, "Oryn"],
        "merge_order": [
            {"repo": target_repo, "action": "review_and_merge_child_pr"},
            {"repo": "Oryn", "action": "update_submodule_pointer_if_required"},
        ],
        "rollback_plan": promotion.get("rollback_plan") or default_rollback_plan(promotion),
        "stable_confirmation_command": confirmation_command,
        "side_effects": _no_side_effects(),
        "generated_at": time.time(),
    }


def _deepswe_package_lines(promotion: Dict[str, Any]) -> list[str]:
    benchmark = promotion.get("benchmark_evidence") or {}
    if not _is_deepswe_evidence(benchmark):
        return []
    summary = _deepswe_package_summary(promotion)
    return [
        "",
        "## DeepSWE Evidence",
        "",
        f"- DeepSWE runs: {', '.join(summary.get('run_ids') or []) or 'none'}",
        f"- Task subset hash: {summary.get('task_subset_hash') or 'missing'}",
        f"- Failure patterns: {', '.join(summary.get('failure_patterns') or []) or 'none recorded'}",
        f"- Action IDs: {', '.join(summary.get('action_ids') or []) or 'none'}",
        "- Raw DeepSWE prompts, reference solutions, verifier patches, and trajectories are intentionally omitted.",
    ]


def _deepswe_package_summary(promotion: Dict[str, Any]) -> Dict[str, Any]:
    benchmark = promotion.get("benchmark_evidence") or {}
    if not _is_deepswe_evidence(benchmark):
        return {}
    candidate = _section(benchmark, "candidate")
    baseline = _section(benchmark, "baseline")
    patterns = benchmark.get("failure_patterns") or candidate.get("failure_patterns") or []
    if isinstance(patterns, list):
        pattern_labels = [
            str((item.get("failure_category") if isinstance(item, dict) else item) or "").strip()
            for item in patterns
            if str((item.get("failure_category") if isinstance(item, dict) else item) or "").strip()
        ]
    else:
        pattern_labels = []
    return {
        "run_ids": _normalize_benchmark_ids(None, benchmark),
        "task_subset_hash": _lookup(candidate, "task_subset_hash") or _lookup(baseline, "task_subset_hash") or benchmark.get("task_subset_hash"),
        "failure_patterns": pattern_labels,
        "action_ids": benchmark.get("action_ids") or candidate.get("action_ids") or [],
    }


def _promotion_values(promotion: Dict[str, Any]) -> tuple[Any, ...]:
    payload = _canonical_json(promotion)
    return (
        promotion["promotion_id"],
        promotion["status"],
        promotion["environment"],
        promotion.get("candidate_id"),
        promotion.get("lab_pass_id"),
        promotion.get("target_repo"),
        promotion.get("target_capability"),
        promotion["improvement_category"],
        _canonical_json(promotion.get("benchmark_run_ids") or []),
        _canonical_json(promotion.get("lab_evidence") or {}),
        _canonical_json(promotion.get("benchmark_evidence") or {}),
        _canonical_json(promotion.get("stable_evidence") or {}),
        _canonical_json(promotion.get("qualification") or {}),
        _canonical_json(promotion.get("package") or {}),
        _canonical_json(promotion.get("blockers") or []),
        _canonical_json(promotion.get("pr_refs") or {}),
        _canonical_json(promotion.get("merge_refs") or {}),
        _canonical_json(promotion.get("rollback_plan") or {}),
        float(promotion["created_at"]),
        float(promotion["updated_at"]),
        payload,
    )


def _promotion_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "object": "hermes.dev_harness_promotion",
        "promotion_id": row["promotion_id"],
        "status": row["status"],
        "environment": row["environment"],
        "candidate_id": row["candidate_id"],
        "lab_pass_id": row["lab_pass_id"],
        "target_repo": row["target_repo"],
        "target_capability": row["target_capability"],
        "improvement_category": row["improvement_category"],
        "benchmark_run_ids": _loads(row["benchmark_run_ids"], []),
        "lab_evidence": _loads(row["lab_evidence"], {}),
        "benchmark_evidence": _loads(row["benchmark_evidence"], {}),
        "stable_evidence": _loads(row["stable_evidence"], {}),
        "qualification": _loads(row["qualification"], {}),
        "package": _loads(row["package"], {}),
        "blockers": _loads(row["blockers"], []),
        "pr_refs": _loads(row["pr_refs"], {}),
        "merge_refs": _loads(row["merge_refs"], {}),
        "rollback_plan": _loads(row["rollback_plan"], {}),
        "created_at": float(row["created_at"]),
        "updated_at": float(row["updated_at"]),
    }


def _provenance_blockers(
    promotion: Dict[str, Any],
    lab_evidence: Dict[str, Any],
    benchmark_evidence: Dict[str, Any],
) -> list[Dict[str, Any]]:
    blockers: list[Dict[str, Any]] = []
    required = {
        "candidate_id": promotion.get("candidate_id") or lab_evidence.get("candidate_id"),
        "lab_pass_id": promotion.get("lab_pass_id") or lab_evidence.get("pass_id"),
        "target_repo": promotion.get("target_repo") or lab_evidence.get("target_repo"),
        "benchmark_evidence": benchmark_evidence,
        "benchmark_run_ids": promotion.get("benchmark_run_ids") or _normalize_benchmark_ids(None, benchmark_evidence),
        "draft_artifact_or_head_sha": _draft_ref(lab_evidence),
    }
    for field, value in required.items():
        if value:
            continue
        blockers.append(_blocker(
            "missing_provenance",
            f"Promotion is missing required provenance field: {field}.",
            field=field,
            severity="blocked",
        ))
    return blockers


def _lab_gate_blockers(lab_evidence: Dict[str, Any]) -> list[Dict[str, Any]]:
    blockers: list[Dict[str, Any]] = []
    if lab_evidence.get("quarantined"):
        blockers.append(_blocker(
            "lab_pass_quarantined",
            "Quarantined Lab pass cannot qualify for Stable promotion.",
            field="lab_evidence.quarantined",
            severity="blocked",
            details={"reason": lab_evidence.get("quarantine_reason")},
        ))
    diff_scope = lab_evidence.get("diff_scope") or {}
    if diff_scope and diff_scope.get("ok") is False:
        blockers.append(_blocker(
            "diff_scope_blocked",
            "Lab diff scope is not allowed for promotion.",
            field="lab_evidence.diff_scope",
            severity="blocked",
            details=diff_scope,
        ))
    if lab_evidence.get("out_of_scope"):
        blockers.append(_blocker(
            "out_of_scope",
            "Out-of-scope Lab evidence cannot qualify for promotion.",
            field="lab_evidence.out_of_scope",
            severity="blocked",
        ))
    if lab_evidence.get("empty_diff"):
        blockers.append(_blocker(
            "empty_diff",
            "Lab pass has no implementation diff to promote.",
            field="lab_evidence.empty_diff",
            severity="blocked",
        ))
    return blockers


def _comparability(benchmark: Dict[str, Any]) -> Dict[str, Any]:
    blockers: list[Dict[str, Any]] = []
    if benchmark.get("comparable") is False:
        blockers.append(_blocker(
            "benchmark_not_comparable",
            "Benchmark evidence declares itself non-comparable.",
            field="benchmark_evidence.comparable",
            severity="inconclusive",
        ))
    before = _section(benchmark, "baseline")
    after = _section(benchmark, "candidate")
    for key in ("task_set_hash", "scoring_rubric", "runtime_profile", "model", "profile_id"):
        left = _lookup(before, key)
        right = _lookup(after, key)
        if left is not None and right is not None and left != right:
            blockers.append(_blocker(
                "benchmark_not_comparable",
                f"Benchmark field {key} differs between baseline and candidate.",
                field=f"benchmark_evidence.{key}",
                severity="inconclusive",
                details={"baseline": left, "candidate": right},
            ))
    if _is_deepswe_evidence(benchmark):
        for key in ("deepswe_commit", "task_subset_hash", "pier_version", "agent_adapter", "model_profile", "network_policy", "scoring_rubric"):
            left = _lookup(before, key)
            right = _lookup(after, key)
            if left is None or right is None:
                blockers.append(_blocker(
                    "deepswe_pinned_context_missing",
                    f"DeepSWE promotion evidence is missing pinned context field {key}.",
                    field=f"benchmark_evidence.{key}",
                    severity="blocked",
                ))
            elif left != right:
                blockers.append(_blocker(
                    "deepswe_not_comparable",
                    f"DeepSWE field {key} differs between baseline and candidate.",
                    field=f"benchmark_evidence.{key}",
                    severity="inconclusive",
                    details={"baseline": left, "candidate": right},
                ))
        infra_rate = _metric(after, "infrastructure_failure_rate")
        if infra_rate is not None and infra_rate >= 0.5:
            blockers.append(_blocker(
                "deepswe_infrastructure_dominated",
                "DeepSWE candidate run is dominated by infrastructure failures.",
                field="benchmark_evidence.infrastructure_failure_rate",
                severity="blocked",
                details={"candidate": infra_rate},
            ))
    return {"ok": not blockers, "blockers": blockers}


def _sample_count(benchmark: Dict[str, Any]) -> int:
    explicit = _first_number(
        benchmark.get("sample_count"),
        benchmark.get("case_count"),
        benchmark.get("task_count"),
        (benchmark.get("summary") or {}).get("sample_count") if isinstance(benchmark.get("summary"), dict) else None,
        (benchmark.get("summary") or {}).get("case_count") if isinstance(benchmark.get("summary"), dict) else None,
    )
    if explicit is not None:
        return int(explicit)
    before = _section(benchmark, "baseline")
    after = _section(benchmark, "candidate")
    return int(min(_section_sample_count(before), _section_sample_count(after)))


def _metric_delta(benchmark: Dict[str, Any], bundle: Dict[str, Any]) -> Dict[str, Any]:
    before = _section(benchmark, "baseline")
    after = _section(benchmark, "candidate")
    metrics = [bundle["primary_metric"], *(bundle.get("alternate_metrics") or [])]
    for metric in metrics:
        baseline = _metric(before, metric)
        candidate = _metric(after, metric)
        if baseline is None or candidate is None:
            continue
        return {
            "metric": metric,
            "baseline": baseline,
            "candidate": candidate,
            "delta": round(candidate - baseline, 6),
            "direction": "higher_is_better",
        }
    return {"metric": None, "baseline": None, "candidate": None, "delta": 0.0}


def _guardrail_blockers(benchmark: Dict[str, Any], bundle: Dict[str, Any]) -> Dict[str, Any]:
    blockers: list[Dict[str, Any]] = []
    deltas: Dict[str, Any] = {}
    before = _section(benchmark, "baseline")
    after = _section(benchmark, "candidate")
    for metric in ("failure_rate",):
        baseline = _metric(before, metric)
        candidate = _metric(after, metric)
        if baseline is None or candidate is None:
            continue
        delta = round(candidate - baseline, 6)
        deltas[metric] = {"metric": metric, "baseline": baseline, "candidate": candidate, "delta": delta}
        if delta > float(bundle["max_failure_rate_regression"]):
            blockers.append(_blocker(
                "guardrail_regressed",
                f"Guardrail {metric} regressed by {delta:.4f}.",
                field=f"benchmark_evidence.{metric}",
                severity="regressed",
                details=deltas[metric],
            ))
    for metric in ("verifier_error_rate", "infrastructure_failure_rate"):
        baseline = _metric(before, metric)
        candidate = _metric(after, metric)
        if baseline is None or candidate is None:
            continue
        delta = round(candidate - baseline, 6)
        deltas[metric] = {"metric": metric, "baseline": baseline, "candidate": candidate, "delta": delta}
        if delta > 0:
            blockers.append(_blocker(
                "guardrail_regressed" if metric == "verifier_error_rate" else "deepswe_infrastructure_regressed",
                f"Guardrail {metric} regressed by {delta:.4f}.",
                field=f"benchmark_evidence.{metric}",
                severity="regressed" if metric == "verifier_error_rate" else "blocked",
                details=deltas[metric],
            ))
    for metric in ("verification_success_rate", "ci_success_rate", "review_success_rate"):
        baseline = _metric(before, metric)
        candidate = _metric(after, metric)
        if baseline is None or candidate is None:
            continue
        delta = round(candidate - baseline, 6)
        deltas[metric] = {"metric": metric, "baseline": baseline, "candidate": candidate, "delta": delta}
        if delta < -float(bundle["max_success_rate_regression"]):
            blockers.append(_blocker(
                "guardrail_regressed",
                f"Guardrail {metric} regressed by {abs(delta):.4f}.",
                field=f"benchmark_evidence.{metric}",
                severity="regressed",
                details=deltas[metric],
            ))
    for metric, max_ratio_key in (("cost_usd", "max_cost_ratio"), ("cost_per_task", "max_cost_ratio"), ("duration_seconds", "max_time_ratio")):
        baseline = _metric(before, metric)
        candidate = _metric(after, metric)
        if baseline is None or candidate is None or baseline <= 0:
            continue
        ratio = round(candidate / baseline, 6)
        deltas[metric] = {"metric": metric, "baseline": baseline, "candidate": candidate, "ratio": ratio}
        if ratio > float(bundle[max_ratio_key]):
            blockers.append(_blocker(
                "budget_regressed",
                f"Budget guardrail {metric} ratio {ratio:.4f} exceeds {bundle[max_ratio_key]:.4f}.",
                field=f"benchmark_evidence.{metric}",
                severity="regressed",
                details=deltas[metric],
            ))
    return {"blockers": blockers, "deltas": deltas}


def _qualification_status(blockers: list[Dict[str, Any]]) -> str:
    severities = {str(blocker.get("severity") or "") for blocker in blockers}
    if "regressed" in severities:
        return "regressed"
    if "blocked" in severities:
        return "blocked"
    if "inconclusive" in severities:
        return "inconclusive"
    return "qualified"


def _fixture_only(benchmark: Dict[str, Any]) -> bool:
    if benchmark.get("fixture_only") or benchmark.get("dry_run_only"):
        return True
    modes = {
        str(value).strip().lower()
        for value in (
            benchmark.get("mode"),
            benchmark.get("benchmark_execution_mode"),
            (_section(benchmark, "baseline") or {}).get("mode"),
            (_section(benchmark, "candidate") or {}).get("mode"),
        )
        if value is not None
    }
    if modes and modes.issubset({"fixture", "dry_run", "dry-run"}):
        return True
    if benchmark.get("live") is False and not benchmark.get("has_live_evidence"):
        return True
    return False


def _section(benchmark: Dict[str, Any], name: str) -> Dict[str, Any]:
    aliases = {
        "baseline": ("baseline", "before", "control"),
        "candidate": ("candidate", "after", "treatment", "experiment"),
    }[name]
    for alias in aliases:
        section = benchmark.get(alias)
        if isinstance(section, dict):
            return section
    return {}


def _section_sample_count(section: Dict[str, Any]) -> int:
    value = _first_number(
        section.get("sample_count"),
        section.get("case_count"),
        section.get("task_count"),
        (section.get("summary") or {}).get("sample_count") if isinstance(section.get("summary"), dict) else None,
        (section.get("summary") or {}).get("task_count") if isinstance(section.get("summary"), dict) else None,
        len(section.get("cases") or []) if isinstance(section.get("cases"), list) else None,
    )
    return int(value or 0)


def _metric(section: Dict[str, Any], metric: str) -> Optional[float]:
    value = _first_number(
        section.get(metric),
        (section.get("metrics") or {}).get(metric) if isinstance(section.get("metrics"), dict) else None,
        (section.get("summary") or {}).get(metric) if isinstance(section.get("summary"), dict) else None,
    )
    return float(value) if value is not None else None


def _lookup(section: Dict[str, Any], key: str) -> Any:
    if key in section:
        return section.get(key)
    for parent in ("pinned_context", "context", "scope", "metadata", "summary"):
        value = section.get(parent)
        if isinstance(value, dict) and key in value:
            return value.get(key)
    return None


def _is_deepswe_evidence(benchmark: Dict[str, Any]) -> bool:
    if str(benchmark.get("provider") or benchmark.get("source") or "").lower() == "deepswe":
        return True
    if benchmark.get("deepswe_run_ids") or benchmark.get("deepswe_run_id"):
        return True
    before = _section(benchmark, "baseline")
    after = _section(benchmark, "candidate")
    return bool(_lookup(before, "deepswe_commit") or _lookup(after, "deepswe_commit"))


def _draft_ref(lab_evidence: Dict[str, Any]) -> Optional[str]:
    draft = lab_evidence.get("draft_artifact") if isinstance(lab_evidence.get("draft_artifact"), dict) else {}
    return _first_str(
        draft.get("artifact_id"),
        draft.get("branch"),
        draft.get("head_sha"),
        lab_evidence.get("branch"),
        lab_evidence.get("head_sha"),
    )


def _normalize_benchmark_ids(value: Any, benchmark_evidence: Dict[str, Any]) -> list[str]:
    ids: list[str] = []
    if isinstance(value, list):
        ids.extend(str(item).strip() for item in value if str(item or "").strip())
    elif value:
        ids.append(str(value).strip())
    for key in ("benchmark_run_id", "baseline_benchmark_run_id", "candidate_benchmark_run_id"):
        item = benchmark_evidence.get(key)
        if item:
            ids.append(str(item).strip())
    for key in ("benchmark_run_ids", "run_ids", "deepswe_run_ids"):
        items = benchmark_evidence.get(key)
        if isinstance(items, list):
            ids.extend(str(item).strip() for item in items if str(item or "").strip())
    for section_name in ("baseline", "candidate", "before", "after"):
        section = benchmark_evidence.get(section_name)
        if isinstance(section, dict):
            item = section.get("run_id") or section.get("deepswe_run_id")
            if item:
                ids.append(str(item).strip())
    seen = set()
    deduped = []
    for item in ids:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _normalize_category(value: Any) -> str:
    normalized = str(value or "output_contract").strip().lower().replace("-", "_").replace("/", "_")
    if normalized in {"ci", "review", "ci_review", "ci_and_review"}:
        return "ci_review"
    if normalized not in THRESHOLD_BUNDLES:
        return "output_contract"
    return normalized


def _blocker(
    code: str,
    message: str,
    *,
    field: Optional[str] = None,
    severity: str = "blocked",
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    blocker = {"code": code, "message": message, "severity": severity}
    if field:
        blocker["field"] = field
    if details:
        blocker["details"] = _compact_evidence(details)
    return blocker


def _compact_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        return "<truncated>"
    if isinstance(value, dict):
        compact: Dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            if key_str in RAW_EVIDENCE_KEYS:
                compact[key_str] = "<omitted>"
                continue
            compact[key_str] = _compact_evidence(item, depth=depth + 1)
        return compact
    if isinstance(value, list):
        return [_compact_evidence(item, depth=depth + 1) for item in value[:50]]
    if isinstance(value, str) and len(value) > 4000:
        return value[:4000] + "...<truncated>"
    return value


def _require_promotion(store: DevHarnessPromotionStore, promotion_id: str) -> Dict[str, Any]:
    promotion = store.get_promotion(promotion_id)
    if promotion is None:
        raise KeyError(f"Dev harness promotion not found: {promotion_id}")
    return promotion


def _no_side_effects() -> Dict[str, bool]:
    return {
        "merged": False,
        "released": False,
        "published": False,
        "branch_protection_changed": False,
        "service_mutated": False,
    }


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
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _extend(target: list[Dict[str, Any]], values: list[Dict[str, Any]]) -> None:
    target.extend(values or [])


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: Any, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return default
