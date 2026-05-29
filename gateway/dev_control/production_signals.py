"""Canonical Hermes back-gate production signal reports and proposals."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from statistics import median
from typing import Any, Dict, Optional

from gateway.dev_control.clarifications import DevClarificationStore, start_clarification
from gateway.dev_control.signal_source import (
    DEFAULT_WINDOW_DAYS,
    DeterministicSignalSource,
    LaminarSignalSource,
    ProductSignalSource,
    SignalWindow,
    cluster_rate,
    default_thresholds,
)
from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_signal_reports (
    report_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    window_start REAL NOT NULL,
    window_end REAL NOT NULL,
    filters TEXT NOT NULL,
    clusters TEXT NOT NULL,
    counts TEXT NOT NULL,
    warnings TEXT NOT NULL,
    health_metrics TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_signal_reports_created_at
    ON dev_signal_reports(created_at DESC);

CREATE TABLE IF NOT EXISTS dev_backlog_proposals (
    proposal_id TEXT PRIMARY KEY,
    report_id TEXT,
    cluster_key TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT NOT NULL,
    evidence_refs TEXT NOT NULL,
    query_descriptor TEXT NOT NULL,
    source_window TEXT NOT NULL,
    seeded_clarification_id TEXT,
    linked_plan_id TEXT,
    outcome TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    reviewed_at REAL,
    promoted_at REAL,
    measured_at REAL
);

CREATE INDEX IF NOT EXISTS idx_dev_backlog_proposals_status
    ON dev_backlog_proposals(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_backlog_proposals_cluster
    ON dev_backlog_proposals(cluster_key, created_at DESC);
"""


class DevProductionSignalStore:
    """Durable reports/proposals for production-signal feedback."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        apply_wal_with_fallback(self._conn, db_label="state.db")
        self._lock = threading.Lock()
        with self._conn:
            self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self._conn.close()

    def create_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_signal_reports (
                    report_id, source, status, window_start, window_end, filters,
                    clusters, counts, warnings, health_metrics, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["report_id"],
                    payload["source"],
                    payload["status"],
                    float(payload["window"]["start"]),
                    float(payload["window"]["end"]),
                    _json(payload.get("filters") or {}),
                    _json(payload.get("clusters") or []),
                    _json(payload.get("counts") or {}),
                    _json(payload.get("warnings") or []),
                    _json(payload.get("health_metrics") or {}),
                    float(payload["created_at"]),
                    float(payload["updated_at"]),
                ),
            )
        return self.get_report(payload["report_id"]) or payload

    def update_report(self, report_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_report(report_id)
        if not current:
            raise KeyError(f"Dev signal report not found: {report_id}")
        merged = {**current, **updates, "updated_at": time.time()}
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_signal_reports
                SET source = ?, status = ?, window_start = ?, window_end = ?,
                    filters = ?, clusters = ?, counts = ?, warnings = ?,
                    health_metrics = ?, created_at = ?, updated_at = ?
                WHERE report_id = ?
                """,
                (
                    merged["source"],
                    merged["status"],
                    float(merged["window"]["start"]),
                    float(merged["window"]["end"]),
                    _json(merged.get("filters") or {}),
                    _json(merged.get("clusters") or []),
                    _json(merged.get("counts") or {}),
                    _json(merged.get("warnings") or []),
                    _json(merged.get("health_metrics") or {}),
                    float(merged["created_at"]),
                    float(merged["updated_at"]),
                    report_id,
                ),
            )
        return self.get_report(report_id) or merged

    def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_signal_reports WHERE report_id = ?",
            (str(report_id or "").strip(),),
        ).fetchone()
        return _report_from_row(row) if row else None

    def list_reports(self, *, limit: int = 50) -> list[Dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM dev_signal_reports
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 50), 200)),),
        ).fetchall()
        return [_report_from_row(row) for row in rows]

    def create_proposal(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_backlog_proposals (
                    proposal_id, report_id, cluster_key, status, payload,
                    evidence_refs, query_descriptor, source_window,
                    seeded_clarification_id, linked_plan_id, outcome,
                    created_at, updated_at, reviewed_at, promoted_at, measured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _proposal_values(payload),
            )
        return self.get_proposal(payload["proposal_id"]) or payload

    def update_proposal(self, proposal_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_proposal(proposal_id)
        if not current:
            raise KeyError(f"Dev backlog proposal not found: {proposal_id}")
        merged = {**current, **updates, "updated_at": time.time()}
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE dev_backlog_proposals
                SET report_id = ?, cluster_key = ?, status = ?, payload = ?,
                    evidence_refs = ?, query_descriptor = ?, source_window = ?,
                    seeded_clarification_id = ?, linked_plan_id = ?, outcome = ?,
                    created_at = ?, updated_at = ?, reviewed_at = ?,
                    promoted_at = ?, measured_at = ?
                WHERE proposal_id = ?
                """,
                (*_proposal_values(merged)[1:], proposal_id),
            )
        return self.get_proposal(proposal_id) or merged

    def get_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_backlog_proposals WHERE proposal_id = ?",
            (str(proposal_id or "").strip(),),
        ).fetchone()
        return _proposal_from_row(row) if row else None

    def find_proposal_by_cluster(self, cluster_key: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM dev_backlog_proposals
            WHERE cluster_key = ? AND status IN ('proposed', 'approved', 'promoted')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (cluster_key,),
        ).fetchone()
        return _proposal_from_row(row) if row else None

    def list_proposals(self, *, status: Optional[str] = None, limit: int = 50) -> list[Dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if status:
            where = "WHERE status = ?"
            params.append(status)
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_backlog_proposals
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [_proposal_from_row(row) for row in rows]


def run_signal_digest(
    *,
    signal_store: DevProductionSignalStore,
    event_store: Any,
    product_event_store: Any = None,
    source: str = "deterministic",
    window_days: Optional[float] = None,
    filters: Optional[Dict[str, Any]] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    return generate_signal_report(
        signal_store=signal_store,
        event_store=event_store,
        product_event_store=product_event_store,
        source=source,
        window_days=window_days,
        filters=filters,
        persist=persist,
        create_proposals=True,
    )


def generate_signal_report(
    *,
    signal_store: DevProductionSignalStore,
    event_store: Any,
    product_event_store: Any = None,
    source: str = "deterministic",
    window_days: Optional[float] = None,
    filters: Optional[Dict[str, Any]] = None,
    persist: bool = True,
    create_proposals: bool = True,
) -> Dict[str, Any]:
    created_at = time.time()
    window = SignalWindow.last_days(window_days, now=created_at)
    filters = filters or {}
    warnings: list[str] = []
    try:
        source_impl = _source_impl(source, event_store=event_store, product_event_store=product_event_store)
        source_result = source_impl.fetch_clusters(window, filters=filters)
        clusters = source_result.get("clusters") or []
        warnings.extend(source_result.get("warnings") or [])
        status = "completed_with_clusters" if clusters else "completed_empty"
    except Exception as exc:
        clusters = []
        status = "analysis_failed"
        warnings.append(f"Signal analysis failed: {exc}")
        source_result = {"source": source, "analyzed_event_count": 0}
    report = {
        "ok": status != "analysis_failed",
        "object": "hermes.dev_signal_report",
        "report_id": f"devsig-{uuid.uuid4().hex[:10]}",
        "source": source_result.get("source") or source,
        "status": status,
        "window": {"start": window.start, "end": window.end, "days": window.days},
        "filters": filters,
        "clusters": clusters,
        "counts": {
            "cluster_count": len(clusters),
            "analyzed_event_count": int(source_result.get("analyzed_event_count") or 0),
            "proposal_count": 0,
        },
        "warnings": warnings,
        "health_metrics": {},
        "created_at": created_at,
        "updated_at": created_at,
        "proposals": [],
    }
    if persist:
        report = signal_store.create_report(report)
    proposals = []
    if create_proposals and status != "analysis_failed":
        proposals = [_create_or_reuse_proposal(signal_store, report, cluster, window) for cluster in clusters]
        report["proposals"] = proposals
        report["counts"]["proposal_count"] = len(proposals)
    report["health_metrics"] = signal_health(signal_store=signal_store, event_store=event_store)
    if persist:
        report = signal_store.update_report(report["report_id"], {
            "counts": report["counts"],
            "health_metrics": report["health_metrics"],
        })
        report["proposals"] = proposals
    return report


def list_signal_reports(*, signal_store: DevProductionSignalStore, limit: int = 50) -> Dict[str, Any]:
    data = signal_store.list_reports(limit=limit)
    return {"ok": True, "object": "list", "data": data, "total": len(data)}


def list_backlog_proposals(*, signal_store: DevProductionSignalStore, status: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    data = signal_store.list_proposals(status=status, limit=limit)
    return {"ok": True, "object": "list", "data": data, "total": len(data)}


def transition_backlog_proposal(
    *,
    signal_store: DevProductionSignalStore,
    clarification_store: Optional[DevClarificationStore],
    proposal_id: str,
    action: str,
    project_id: Optional[str] = None,
) -> Dict[str, Any]:
    proposal = signal_store.get_proposal(proposal_id)
    if not proposal:
        raise KeyError(f"Dev backlog proposal not found: {proposal_id}")
    now = time.time()
    if action in {"approve", "dismiss"}:
        status = "approved" if action == "approve" else "dismissed"
        payload = {**proposal.get("payload", {}), "status": status}
        return signal_store.update_proposal(proposal_id, {
            "status": status,
            "payload": payload,
            "reviewed_at": now,
        })
    if action != "promote":
        raise ValueError(f"Unsupported proposal action: {action}")
    if clarification_store is None:
        raise ValueError("Clarification store is required to promote a proposal.")
    clarification = start_clarification(
        store=clarification_store,
        vision_brief=_promotion_brief(proposal),
        project_id=project_id,
        project_context={
            "project_name": project_id or "Oryn Workspace",
            "work_items": [proposal.get("payload", {}).get("title") or proposal.get("cluster_key")],
            "production_signal": {
                "proposal_id": proposal_id,
                "cluster_key": proposal.get("cluster_key"),
                "evidence_refs": proposal.get("evidence_refs") or [],
            },
        },
        max_questions=3,
    )
    payload = {**proposal.get("payload", {}), "status": "promoted"}
    return signal_store.update_proposal(proposal_id, {
        "status": "promoted",
        "payload": payload,
        "reviewed_at": proposal.get("reviewed_at") or now,
        "promoted_at": now,
        "seeded_clarification_id": clarification["clarification_id"],
    })


def measure_proposal_outcome(
    *,
    signal_store: DevProductionSignalStore,
    event_store: Any,
    product_event_store: Any = None,
    proposal_id: str,
    window_days: Optional[float] = None,
    source: str = "deterministic",
) -> Dict[str, Any]:
    proposal = signal_store.get_proposal(proposal_id)
    if not proposal:
        raise KeyError(f"Dev backlog proposal not found: {proposal_id}")
    now = time.time()
    after_window = SignalWindow.last_days(window_days or (proposal.get("source_window") or {}).get("days") or DEFAULT_WINDOW_DAYS, now=now)
    source_result = _source_impl(source, event_store=event_store, product_event_store=product_event_store).fetch_clusters(after_window, filters={})
    cluster_key = proposal.get("cluster_key")
    after_cluster = next((item for item in source_result.get("clusters") or [] if item.get("key") == cluster_key), None)
    source_window = proposal.get("source_window") or {}
    before_rate = float(source_window.get("rate_per_day") or cluster_rate({"count": source_window.get("count") or 0}, SignalWindow(
        start=float(source_window.get("start") or now - DEFAULT_WINDOW_DAYS * 86400),
        end=float(source_window.get("end") or now),
    )))
    after_rate = cluster_rate(after_cluster, after_window)
    outcome = {
        "before_rate": round(before_rate, 4),
        "after_rate": round(after_rate, 4),
        "before_count": int(source_window.get("count") or 0),
        "after_count": int((after_cluster or {}).get("count") or 0),
        "window": {"start": after_window.start, "end": after_window.end, "days": after_window.days},
        "warnings": source_result.get("warnings") or [],
        "status": _outcome_status(before_rate, after_rate),
        "measured_at": now,
    }
    return signal_store.update_proposal(proposal_id, {
        "outcome": outcome,
        "measured_at": now,
    })


def signal_health(*, signal_store: DevProductionSignalStore, event_store: Any = None) -> Dict[str, Any]:
    reports = signal_store.list_reports(limit=50)
    proposals = signal_store.list_proposals(limit=200)
    latest = reports[0] if reports else None
    status_counts = _count_by(proposals, "status")
    now = time.time()
    aging_days = int(default_thresholds()["proposal_aging_days"])
    aging = [
        proposal for proposal in proposals
        if proposal.get("status") in {"proposed", "approved"}
        and now - float(proposal.get("created_at") or now) > aging_days * 86400
    ]
    reviewed_durations = [
        float(proposal.get("reviewed_at")) - float(proposal.get("created_at"))
        for proposal in proposals
        if proposal.get("reviewed_at") and proposal.get("created_at")
    ]
    promoted = [proposal for proposal in proposals if proposal.get("status") == "promoted"]
    measured = [proposal for proposal in promoted if (proposal.get("outcome") or {}).get("measured_at")]
    improved = [proposal for proposal in measured if (proposal.get("outcome") or {}).get("status") == "improved"]
    regressed = [proposal for proposal in measured if (proposal.get("outcome") or {}).get("status") == "regressed"]
    no_change = [proposal for proposal in measured if (proposal.get("outcome") or {}).get("status") == "no_change"]
    analyzed = sum(int((report.get("counts") or {}).get("analyzed_event_count") or 0) for report in reports)
    clusters = sum(int((report.get("counts") or {}).get("cluster_count") or 0) for report in reports)
    return {
        "object": "hermes.dev_signal_health",
        "last_analyzed_at": latest.get("created_at") if latest else None,
        "last_analysis_status": latest.get("status") if latest else "never_run",
        "coverage": {
            "analyzed_event_count": analyzed,
            "cluster_count": clusters,
            "latest_window": latest.get("window") if latest else None,
        },
        "conversion_rate": round((len(proposals) / clusters), 3) if clusters else 0.0,
        "proposals_by_status": status_counts,
        "open_proposal_count": int(status_counts.get("proposed") or 0) + int(status_counts.get("approved") or 0),
        "aging_proposal_count": len(aging),
        "median_time_to_review_seconds": round(median(reviewed_durations), 3) if reviewed_durations else None,
        "outcome_coverage": {
            "promoted": len(promoted),
            "awaiting_measurement": max(len(promoted) - len(measured), 0),
            "measured": len(measured),
            "improved": len(improved),
            "no_change": len(no_change),
            "regressed": len(regressed),
        },
    }


def _source_impl(source: str, *, event_store: Any, product_event_store: Any = None) -> Any:
    normalized = str(source or "").lower()
    if normalized == "laminar":
        return LaminarSignalSource()
    if normalized == "product":
        if product_event_store is None:
            from gateway.dev_control.product_events import DevProductEventStore
            db_path = getattr(event_store, "db_path", None)
            product_event_store = DevProductEventStore(db_path)
        return ProductSignalSource(product_event_store)
    return DeterministicSignalSource(event_store)


def _create_or_reuse_proposal(signal_store: DevProductionSignalStore, report: Dict[str, Any], cluster: Dict[str, Any], window: SignalWindow) -> Dict[str, Any]:
    existing = signal_store.find_proposal_by_cluster(cluster["key"])
    if existing:
        return existing
    now = time.time()
    proposal = {
        "proposal_id": f"devprop-{uuid.uuid4().hex[:10]}",
        "report_id": report["report_id"],
        "cluster_key": cluster["key"],
        "status": "proposed",
        "payload": _proposal_payload(cluster),
        "evidence_refs": cluster.get("evidence_refs") or [],
        "query_descriptor": cluster.get("query_descriptor") or {"cluster_key": cluster["key"]},
        "source_window": {
            "start": window.start,
            "end": window.end,
            "days": window.days,
            "count": cluster.get("count") or 0,
            "rate_per_day": cluster.get("rate_per_day") or cluster_rate(cluster, window),
        },
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


def _proposal_payload(cluster: Dict[str, Any]) -> Dict[str, Any]:
    title = cluster.get("title") or cluster.get("key") or "Production signal"
    return {
        "title": title,
        "category": "production_signal",
        "priority": _priority_for_cluster(cluster),
        "impact": f"{cluster.get('count', 0)} matching production signal(s) in the analysis window.",
        "risk": "Proposal is advisory; review evidence before creating work.",
        "affected_components": [str(cluster.get("key") or "production_signal")],
        "evidence_refs": cluster.get("evidence_refs") or [],
        "reason": "Hermes observed repeated production signals.",
        "suggested_change": f"Review and clarify a fix for: {title}",
        "non_goals": ["Do not mutate runtime policy or auto-create execution work from this proposal."],
        "source": "production_signal",
        "status": "proposed",
    }


def _priority_for_cluster(cluster: Dict[str, Any]) -> str:
    count = int(cluster.get("count") or 0)
    key = str(cluster.get("key") or "")
    if "failed" in key or count >= 5:
        return "high"
    if "unverifiable" in key or "low_" in key or count >= 3:
        return "medium"
    return "low"


def _promotion_brief(proposal: Dict[str, Any]) -> str:
    payload = proposal.get("payload") or {}
    evidence = proposal.get("evidence_refs") or []
    return "\n".join([
        f"Production signal proposal: {payload.get('title') or proposal.get('cluster_key')}",
        "",
        str(payload.get("reason") or ""),
        str(payload.get("impact") or ""),
        "",
        "Suggested change:",
        str(payload.get("suggested_change") or ""),
        "",
        "Evidence refs:",
        json.dumps(evidence[:10], ensure_ascii=False),
    ]).strip()


def _outcome_status(before_rate: float, after_rate: float) -> str:
    if after_rate < before_rate:
        return "improved"
    if after_rate > before_rate:
        return "regressed"
    return "no_change"


def _count_by(items: list[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _report_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "ok": row["status"] != "analysis_failed",
        "object": "hermes.dev_signal_report",
        "report_id": row["report_id"],
        "source": row["source"],
        "status": row["status"],
        "window": {"start": row["window_start"], "end": row["window_end"], "days": max((row["window_end"] - row["window_start"]) / 86400, 0.1)},
        "filters": _loads(row["filters"], {}),
        "clusters": _loads(row["clusters"], []),
        "counts": _loads(row["counts"], {}),
        "warnings": _loads(row["warnings"], []),
        "health_metrics": _loads(row["health_metrics"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _proposal_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "object": "hermes.dev_backlog_proposal",
        "proposal_id": row["proposal_id"],
        "report_id": row["report_id"],
        "cluster_key": row["cluster_key"],
        "status": row["status"],
        "payload": _loads(row["payload"], {}),
        "evidence_refs": _loads(row["evidence_refs"], []),
        "query_descriptor": _loads(row["query_descriptor"], {}),
        "source_window": _loads(row["source_window"], {}),
        "seeded_clarification_id": row["seeded_clarification_id"],
        "linked_plan_id": row["linked_plan_id"],
        "outcome": _loads(row["outcome"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "reviewed_at": row["reviewed_at"],
        "promoted_at": row["promoted_at"],
        "measured_at": row["measured_at"],
    }


def _proposal_values(payload: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["proposal_id"],
        payload.get("report_id"),
        payload["cluster_key"],
        payload["status"],
        _json(payload.get("payload") or {}),
        _json(payload.get("evidence_refs") or []),
        _json(payload.get("query_descriptor") or {}),
        _json(payload.get("source_window") or {}),
        payload.get("seeded_clarification_id"),
        payload.get("linked_plan_id"),
        _json(payload.get("outcome") or {}),
        payload.get("created_at"),
        payload.get("updated_at"),
        payload.get("reviewed_at"),
        payload.get("promoted_at"),
        payload.get("measured_at"),
    )


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: Any, default: Any) -> Any:
    try:
        return json.loads(value or _json(default))
    except Exception:
        return default
