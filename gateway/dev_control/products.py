"""Durable Dev Product spine for long-running product management."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from gateway.dev_control.project_scope import DEFAULT_PROJECT_ID, resolve_project_id
from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_products (
    product_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    primary_repo TEXT,
    repository_bindings TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    archived_at REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dev_products_active_project
    ON dev_products(project_id)
    WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_dev_products_lifecycle
    ON dev_products(lifecycle_state, updated_at DESC);

CREATE TABLE IF NOT EXISTS dev_product_progression_loop_iterations (
    iteration_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    selected_action_id TEXT,
    selected_action_kind TEXT,
    source_refs TEXT NOT NULL,
    payload TEXT NOT NULL,
    evaluated_at REAL NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_product_progression_product_time
    ON dev_product_progression_loop_iterations(product_id, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_product_progression_status_time
    ON dev_product_progression_loop_iterations(status, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS dev_portfolio_flow_control (
    scope_id TEXT PRIMARY KEY,
    flow_control TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""

PRODUCT_LIFECYCLE_STATES = {
    "unknown",
    "framing",
    "planned",
    "active",
    "paused",
    "blocked",
    "shipping",
    "complete",
    "archived",
}

PRODUCT_FLOW_CONTROL_STATES = {
    "unknown",
    "normal",
    "paused",
    "hold_new_work",
    "needs_direction",
}
PRODUCT_FLOW_CONTROL_MUTATION_STATES = PRODUCT_FLOW_CONTROL_STATES - {"unknown"}
PRODUCT_AUTONOMY_LEVELS = {
    "manual",
    "supervised",
    "bounded",
}
DEFAULT_PRODUCT_AUTONOMY_LEVEL = "supervised"
PORTFOLIO_FLOW_CONTROL_SCOPE_ID = "default"
PRODUCT_FLOW_CONTROL_HISTORY_LIMIT = 20
PRODUCT_ACTION_ATTENTION_STATES = {"failed", "blocked", "needs_attention"}
PRODUCT_VISION_ALIGNMENT_STATES = {"on_track", "at_risk", "off_track", "unassessed"}
PRODUCT_PROGRESSION_STATUSES = {
    "advanced",
    "blocked",
    "waiting_for_human",
    "held_by_flow_control",
    "idle",
}

BACKLOG_TERMINAL_STATES = {"complete", "failed"}
BACKLOG_ACTIVE_STATES = {"in_flight", "needs_attention", "blocked"}


@dataclass
class DevProductStore:
    """Persistence for durable Dev Products."""

    db_path: Optional[Path] = None

    def __post_init__(self) -> None:
        self.db_path = self.db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        apply_wal_with_fallback(self._conn, db_label="state.db")
        with self._conn:
            self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self._conn.close()

    def create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = _normalize_product_payload(payload)
        now = float(normalized.get("created_at") or time.time())
        normalized["created_at"] = now
        normalized["updated_at"] = float(normalized.get("updated_at") or now)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_products (
                    product_id, project_id, name, lifecycle_state, primary_repo,
                    repository_bindings, payload, created_at, updated_at,
                    archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _row_values(normalized),
            )
        return self.get(normalized["product_id"]) or normalized

    def upsert(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = _normalize_product_payload(payload, deterministic_id=True)
        existing = self.get_by_project_id(normalized["project_id"])
        if existing:
            normalized["product_id"] = existing["product_id"]
            normalized["created_at"] = existing["created_at"]
            normalized["updated_at"] = time.time()
            if "archived_at" not in payload:
                normalized["archived_at"] = existing.get("archived_at")
        else:
            now = time.time()
            normalized["created_at"] = float(normalized.get("created_at") or now)
            normalized["updated_at"] = float(normalized.get("updated_at") or now)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_products (
                    product_id, project_id, name, lifecycle_state, primary_repo,
                    repository_bindings, payload, created_at, updated_at,
                    archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    project_id = excluded.project_id,
                    name = excluded.name,
                    lifecycle_state = excluded.lifecycle_state,
                    primary_repo = excluded.primary_repo,
                    repository_bindings = excluded.repository_bindings,
                    payload = excluded.payload,
                    updated_at = excluded.updated_at,
                    archived_at = excluded.archived_at
                """,
                _row_values(normalized),
            )
        return self.get(normalized["product_id"]) or normalized

    def get(self, product_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_products WHERE product_id = ?",
            (str(product_id or "").strip(),),
        ).fetchone()
        return _row_to_payload(row) if row else None

    def get_by_project_id(self, project_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM dev_products
            WHERE project_id = ? AND archived_at IS NULL
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (resolve_project_id(project_id, default=DEFAULT_PROJECT_ID),),
        ).fetchone()
        return _row_to_payload(row) if row else None

    def list(
        self,
        *,
        include_archived: bool = False,
        lifecycle_state: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if not include_archived:
            clauses.append("archived_at IS NULL")
        if lifecycle_state is not None:
            clauses.append("lifecycle_state = ?")
            params.append(normalize_lifecycle_state(lifecycle_state))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 100), 500)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_products
            {where}
            ORDER BY updated_at DESC, name ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_row_to_payload(row) for row in rows]

    def archive(self, product_id: str) -> Dict[str, Any]:
        current = self.get(product_id)
        if not current:
            raise KeyError(f"Dev Product not found: {product_id}")
        archived_at = time.time()
        with self._conn:
            self._conn.execute(
                """
                UPDATE dev_products
                SET lifecycle_state = 'archived', archived_at = ?, updated_at = ?
                WHERE product_id = ?
                """,
                (archived_at, archived_at, product_id),
            )
        return self.get(product_id) or {**current, "lifecycle_state": "archived", "archived_at": archived_at}

    def update_flow_control(
        self,
        product_id: str,
        *,
        state: str,
        autonomy_level: Optional[str] = None,
        reason: Optional[str] = None,
        requested_by: Optional[str] = None,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        current = self.get(product_id)
        if not current:
            raise KeyError(f"Dev Product not found: {product_id}")
        flow_control = build_flow_control_update(
            state=state,
            autonomy_level=autonomy_level,
            reason=reason,
            requested_by=requested_by,
            previous=current.get("flow_control"),
            now=now,
        )
        product_payload = dict(current.get("payload") or {})
        history = product_payload.get("flow_control_history")
        if not isinstance(history, list):
            history = []
        history.append(flow_control)
        product_payload["flow_control"] = flow_control
        product_payload["flow_control_history"] = history[-PRODUCT_FLOW_CONTROL_HISTORY_LIMIT:]
        updated_at = float(flow_control["updated_at"])
        with self._conn:
            self._conn.execute(
                """
                UPDATE dev_products
                SET payload = ?, updated_at = ?
                WHERE product_id = ?
                """,
                (json.dumps(product_payload, ensure_ascii=False), updated_at, current["product_id"]),
        )
        return self.get(current["product_id"]) or {**current, "payload": product_payload, "flow_control": flow_control}

    def get_portfolio_flow_control(self) -> Dict[str, Any]:
        row = self._conn.execute(
            """
            SELECT flow_control
            FROM dev_portfolio_flow_control
            WHERE scope_id = ?
            """,
            (PORTFOLIO_FLOW_CONTROL_SCOPE_ID,),
        ).fetchone()
        if not row:
            return default_flow_control(scope="portfolio")
        try:
            raw = json.loads(row["flow_control"] or "{}")
        except Exception:
            raw = {}
        return normalize_flow_control(raw, scope="portfolio")

    def update_portfolio_flow_control(
        self,
        *,
        state: str,
        autonomy_level: Optional[str] = None,
        reason: Optional[str] = None,
        requested_by: Optional[str] = None,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        previous = self.get_portfolio_flow_control()
        flow_control = build_flow_control_update(
            state=state,
            autonomy_level=autonomy_level,
            reason=reason,
            requested_by=requested_by,
            previous=previous,
            scope="portfolio",
            now=now,
        )
        updated_at = float(flow_control["updated_at"])
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_portfolio_flow_control (scope_id, flow_control, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(scope_id) DO UPDATE SET
                    flow_control = excluded.flow_control,
                    updated_at = excluded.updated_at
                """,
                (
                    PORTFOLIO_FLOW_CONTROL_SCOPE_ID,
                    json.dumps(flow_control, ensure_ascii=False),
                    updated_at,
                ),
            )
        return self.get_portfolio_flow_control()

    def record_progression_iteration(self, iteration: Dict[str, Any]) -> Dict[str, Any]:
        normalized = _normalize_progression_iteration(iteration)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_product_progression_loop_iterations (
                    iteration_id, product_id, project_id, status, reason,
                    selected_action_id, selected_action_kind, source_refs,
                    payload, evaluated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["iteration_id"],
                    normalized["product_id"],
                    normalized["project_id"],
                    normalized["status"],
                    normalized.get("reason"),
                    normalized.get("selected_action_id"),
                    normalized.get("selected_action_kind"),
                    json.dumps(normalized.get("source_refs") or [], ensure_ascii=False),
                    json.dumps(normalized.get("payload") or {}, ensure_ascii=False),
                    float(normalized["evaluated_at"]),
                    float(normalized["created_at"]),
                ),
            )
        return self.get_progression_iteration(normalized["iteration_id"]) or normalized

    def get_progression_iteration(self, iteration_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM dev_product_progression_loop_iterations
            WHERE iteration_id = ?
            """,
            (str(iteration_id or "").strip(),),
        ).fetchone()
        return _progression_row_to_payload(row) if row else None

    def list_progression_iterations(
        self,
        *,
        product_id: Optional[str] = None,
        latest_only: bool = True,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        bounded = max(1, min(int(limit or 100), 500))
        params: list[Any] = []
        if latest_only:
            where = ""
            if product_id:
                where = "WHERE latest.product_id = ?"
                params.append(str(product_id).strip())
            rows = self._conn.execute(
                f"""
                SELECT iteration.*
                FROM dev_product_progression_loop_iterations iteration
                JOIN (
                    SELECT product_id, MAX(evaluated_at) AS evaluated_at
                    FROM dev_product_progression_loop_iterations
                    GROUP BY product_id
                ) latest
                    ON latest.product_id = iteration.product_id
                   AND latest.evaluated_at = iteration.evaluated_at
                {where}
                ORDER BY iteration.evaluated_at DESC, iteration.product_id ASC, iteration.iteration_id ASC
                LIMIT ?
                """,
                tuple([*params, bounded]),
            ).fetchall()
        else:
            where = ""
            if product_id:
                where = "WHERE product_id = ?"
                params.append(str(product_id).strip())
            rows = self._conn.execute(
                f"""
                SELECT *
                FROM dev_product_progression_loop_iterations
                {where}
                ORDER BY evaluated_at DESC, product_id ASC, iteration_id ASC
                LIMIT ?
                """,
                tuple([*params, bounded]),
            ).fetchall()
        return [_progression_row_to_payload(row) for row in rows]


def create_product(
    *,
    store: DevProductStore,
    project_id: Optional[str] = None,
    name: Optional[str] = None,
    lifecycle_state: Optional[str] = None,
    primary_repo: Optional[str] = None,
    repository_bindings: Optional[Iterable[Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    product_id: Optional[str] = None,
) -> Dict[str, Any]:
    return store.create({
        "product_id": product_id,
        "project_id": project_id,
        "name": name,
        "lifecycle_state": lifecycle_state,
        "primary_repo": primary_repo,
        "repository_bindings": list(repository_bindings or []),
        "payload": payload or {},
    })


def upsert_product(
    *,
    store: DevProductStore,
    project_id: Optional[str] = None,
    name: Optional[str] = None,
    lifecycle_state: Optional[str] = None,
    primary_repo: Optional[str] = None,
    repository_bindings: Optional[Iterable[Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    product_id: Optional[str] = None,
) -> Dict[str, Any]:
    return store.upsert({
        "product_id": product_id,
        "project_id": project_id,
        "name": name,
        "lifecycle_state": lifecycle_state,
        "primary_repo": primary_repo,
        "repository_bindings": list(repository_bindings or []),
        "payload": payload or {},
    })


def update_product_flow_control(
    *,
    store: DevProductStore,
    product_id: str,
    state: str,
    autonomy_level: Optional[str] = None,
    reason: Optional[str] = None,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    return store.update_flow_control(
        product_id,
        state=state,
        autonomy_level=autonomy_level,
        reason=reason,
        requested_by=requested_by,
    )


def update_portfolio_flow_control(
    *,
    store: DevProductStore,
    state: str,
    autonomy_level: Optional[str] = None,
    reason: Optional[str] = None,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    return store.update_portfolio_flow_control(
        state=state,
        autonomy_level=autonomy_level,
        reason=reason,
        requested_by=requested_by,
    )


def seed_products_from_project_ids(
    *,
    store: DevProductStore,
    project_ids: Iterable[str],
    workspace_projects: Optional[Iterable[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Create missing Products from existing project ids and optional app framing."""

    framing_by_project_id: Dict[str, Dict[str, Any]] = {}
    for project in workspace_projects or []:
        project_id = resolve_project_id(
            project.get("hermes_project_id") or project.get("project_id"),
            default=DEFAULT_PROJECT_ID,
        )
        framing_by_project_id[project_id] = dict(project)

    seeded: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_project_id in project_ids:
        project_id = resolve_project_id(raw_project_id, default=DEFAULT_PROJECT_ID)
        if project_id in seen:
            continue
        seen.add(project_id)
        framing = framing_by_project_id.get(project_id, {})
        if not framing and (existing := store.get_by_project_id(project_id)):
            seeded.append(existing)
            continue
        repos = framing.get("repos") or framing.get("repository_bindings") or []
        primary_repo = _primary_repo_from_bindings(repos)
        seeded.append(store.upsert({
            "product_id": stable_product_id_for_project(project_id),
            "project_id": project_id,
            "name": framing.get("name") or _display_name_from_project_id(project_id),
            "lifecycle_state": framing.get("lifecycle_state") or "unknown",
            "primary_repo": primary_repo,
            "repository_bindings": repos,
            "payload": {
                "source": "migration",
                "workspace_project_id": str(framing.get("id") or "") or None,
                "vision": framing.get("vision"),
                "coordinator_profile": framing.get("coordinator_profile"),
            },
        }))
    return seeded


def build_product_backlog(
    *,
    product: Dict[str, Any],
    execution_store: Any,
    goal_store: Any = None,
    verification_store: Any = None,
    incident_store: Any = None,
    event_store: Any = None,
    manual_work_items: Optional[Iterable[Dict[str, Any]]] = None,
    plan_limit: int = 100,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build a Product backlog read model from Hermes execution truth."""

    project_id = resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID)
    updated_at = float(now or time.time())
    goals = _list_goals(goal_store, project_id=project_id)
    goals_by_id = {goal["goal_id"]: goal for goal in goals if goal.get("goal_id")}
    milestone_by_id = {
        goal["goal_id"]: goal
        for goal in goals
        if goal.get("kind") == "milestone" and goal.get("goal_id")
    }
    manual_context = _manual_context_by_source(manual_work_items or [])
    incidents = _list_incidents(incident_store)
    items: list[Dict[str, Any]] = []
    linked_goal_ids: set[str] = set()

    for plan_index, plan in enumerate(_list_execution_plans(execution_store, project_id=project_id, limit=plan_limit)):
        launch_records = _list_launch_records(execution_store, plan.get("plan_id"))
        for task_index, task in enumerate(plan.get("tasks") or []):
            if resolve_project_id(task.get("project_id"), default=project_id) != project_id:
                continue
            item = _backlog_item_from_task(
                product=product,
                plan=plan,
                task=task,
                plan_index=plan_index,
                task_index=task_index,
                goals_by_id=goals_by_id,
                milestone_by_id=milestone_by_id,
                launch_records=launch_records,
                verification_store=verification_store,
                incident_records=incidents,
                event_store=event_store,
                manual_context=manual_context,
            )
            if item.get("linked_goal_id"):
                linked_goal_ids.add(str(item["linked_goal_id"]))
            items.append(item)

    for goal in goals:
        if goal.get("kind") != "subgoal":
            continue
        goal_id = str(goal.get("goal_id") or "").strip()
        if not goal_id or goal_id in linked_goal_ids:
            continue
        items.append(_backlog_item_from_goal(
            product=product,
            goal=goal,
            goals_by_id=goals_by_id,
            milestone_by_id=milestone_by_id,
            manual_context=manual_context,
        ))

    items.sort(key=lambda item: (item.get("ordering") or 0, item.get("updated_at") or 0, item.get("id") or ""))
    for index, item in enumerate(items, start=1):
        item["ordering"] = index
    milestone_groups = _milestone_groups(items, milestone_by_id)
    counts = _backlog_counts(items)
    next_item_id = _next_actionable_item_id(items)
    latest_source_at = max(
        [float(item.get("freshness", {}).get("latest_source_at") or 0) for item in items] + [0.0],
    )
    return {
        "object": "hermes.dev_product_backlog",
        "product_id": product.get("product_id"),
        "project_id": project_id,
        "flow_control": product_flow_control(product),
        "updated_at": updated_at,
        "freshness": {
            "updated_at": updated_at,
            "latest_source_at": latest_source_at or None,
            "source_count": len(items),
        },
        "counts": counts,
        "next_item_id": next_item_id,
        "milestone_groups": milestone_groups,
        "items": items,
    }


def build_product_portfolio(
    *,
    store: DevProductStore,
    execution_store: Any,
    goal_store: Any = None,
    verification_store: Any = None,
    incident_store: Any = None,
    event_store: Any = None,
    clarification_store: Any = None,
    plan_artifact_store: Any = None,
    product_limit: int = 100,
    plan_limit: int = 100,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build a compact read-only portfolio overview across active Products."""

    updated_at = float(now or time.time())
    products = store.list(include_archived=False, limit=product_limit)
    items: list[Dict[str, Any]] = []
    for product in products:
        try:
            backlog = build_product_backlog(
                product=product,
                execution_store=execution_store,
                goal_store=goal_store,
                verification_store=verification_store,
                incident_store=incident_store,
                event_store=event_store,
                plan_limit=plan_limit,
                now=updated_at,
            )
        except Exception as exc:
            backlog = _unknown_backlog_for_product(product, updated_at=updated_at, reason=str(exc))
        action_surface = build_product_action_surface_for_product(
            product=product,
            backlog=backlog,
            execution_store=execution_store,
            clarification_store=clarification_store,
            plan_artifact_store=plan_artifact_store,
            now=updated_at,
            action_limit=5,
        )
        items.append(_portfolio_item_from_product(product, backlog, action_surface=action_surface))
    items.sort(key=_portfolio_sort_key)
    for index, item in enumerate(items, start=1):
        item["ordering"] = index
    flow_control = store.get_portfolio_flow_control()
    return {
        "object": "hermes.dev_product_portfolio",
        "updated_at": updated_at,
        "flow_control": flow_control,
        "total": len(items),
        "counts": _portfolio_counts(items),
        "items": items,
    }


def build_product_action_surface(
    *,
    store: DevProductStore,
    execution_store: Any,
    clarification_store: Any = None,
    plan_artifact_store: Any = None,
    goal_store: Any = None,
    verification_store: Any = None,
    incident_store: Any = None,
    event_store: Any = None,
    product_id: Optional[str] = None,
    product_limit: int = 100,
    action_limit: int = 20,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build a read-only Product action surface from existing Hermes evidence."""

    updated_at = float(now or time.time())
    if product_id:
        product = store.get(product_id)
        products = [product] if product else []
    else:
        products = store.list(include_archived=False, limit=product_limit)
    items: list[Dict[str, Any]] = []
    for product in products:
        try:
            backlog = build_product_backlog(
                product=product,
                execution_store=execution_store,
                goal_store=goal_store,
                verification_store=verification_store,
                incident_store=incident_store,
                event_store=event_store,
                now=updated_at,
            )
        except Exception as exc:
            backlog = _unknown_backlog_for_product(product, updated_at=updated_at, reason=str(exc))
        items.extend(build_product_action_surface_for_product(
            product=product,
            backlog=backlog,
            execution_store=execution_store,
            clarification_store=clarification_store,
            plan_artifact_store=plan_artifact_store,
            now=updated_at,
            action_limit=action_limit,
        )["actions"])
    items.sort(key=_product_action_sort_key)
    for index, item in enumerate(items, start=1):
        item["ordering"] = index
    return {
        "object": "hermes.dev_product_actions",
        "updated_at": updated_at,
        "total": len(items),
        "counts": _product_action_counts(items),
        "data": items,
    }


def build_product_action_surface_for_product(
    *,
    product: Dict[str, Any],
    backlog: Dict[str, Any],
    execution_store: Any,
    clarification_store: Any = None,
    plan_artifact_store: Any = None,
    now: Optional[float] = None,
    action_limit: int = 20,
) -> Dict[str, Any]:
    project_id = resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID)
    updated_at = float(now or time.time())
    actions: list[Dict[str, Any]] = []
    actions.extend(_flow_control_actions(product, updated_at=updated_at))
    actions.extend(_clarification_actions(product, clarification_store))
    actions.extend(_draft_review_actions(product, execution_store))
    actions.extend(_supervisor_approval_actions(product, execution_store))
    actions.extend(_backlog_attention_actions(product, backlog))
    actions.sort(key=_product_action_sort_key)
    actions = actions[:max(1, min(int(action_limit or 20), 100))]
    for index, item in enumerate(actions, start=1):
        item["ordering"] = index
    return {
        "object": "hermes.dev_product_action_summary",
        "product_id": product.get("product_id"),
        "project_id": project_id,
        "updated_at": updated_at,
        "counts": _product_action_counts(actions),
        "next_action": actions[0] if actions else None,
        "actions": actions,
    }


def list_product_progression_loop(
    *,
    store: DevProductStore,
    product_id: Optional[str] = None,
    limit: int = 100,
    latest_only: bool = True,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """List Product progression loop state without mutating Product evidence."""

    items = store.list_progression_iterations(
        product_id=product_id,
        latest_only=latest_only,
        limit=limit,
    )
    items.sort(key=_product_progression_sort_key)
    for index, item in enumerate(items, start=1):
        item["ordering"] = index
    return {
        "object": "hermes.dev_product_progression_loop",
        "updated_at": float(now or time.time()),
        "total": len(items),
        "counts": _product_progression_counts(items),
        "data": items,
    }


def build_product_live_portfolio_snapshot(
    *,
    store: DevProductStore,
    execution_store: Any,
    clarification_store: Any = None,
    plan_artifact_store: Any = None,
    goal_store: Any = None,
    verification_store: Any = None,
    incident_store: Any = None,
    event_store: Any = None,
    product_limit: int = 100,
    plan_limit: int = 100,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build one read-only Product portfolio snapshot for live console streams."""

    emitted_at = float(now or time.time())
    portfolio = build_product_portfolio(
        store=store,
        execution_store=execution_store,
        goal_store=goal_store,
        verification_store=verification_store,
        incident_store=incident_store,
        event_store=event_store,
        clarification_store=clarification_store,
        plan_artifact_store=plan_artifact_store,
        product_limit=product_limit,
        plan_limit=plan_limit,
        now=emitted_at,
    )
    actions = build_product_action_surface(
        store=store,
        execution_store=execution_store,
        clarification_store=clarification_store,
        plan_artifact_store=plan_artifact_store,
        goal_store=goal_store,
        verification_store=verification_store,
        incident_store=incident_store,
        event_store=event_store,
        product_limit=product_limit,
        now=emitted_at,
    )
    progression = list_product_progression_loop(
        store=store,
        limit=product_limit,
        latest_only=True,
        now=emitted_at,
    )
    progression_updated_at = max(
        [_float_or_zero(item.get("evaluated_at")) for item in progression.get("data") or []] + [0.0],
    ) or None
    return {
        "object": "hermes.dev_product_live_portfolio_snapshot",
        "event": "dev.product.portfolio.snapshot",
        "emitted_at": emitted_at,
        "portfolio": portfolio,
        "flow_control": portfolio.get("flow_control") or store.get_portfolio_flow_control(),
        "actions": actions,
        "progression_loop": progression,
        "freshness": {
            "emitted_at": emitted_at,
            "portfolio_updated_at": portfolio.get("updated_at"),
            "actions_updated_at": actions.get("updated_at"),
            "progression_updated_at": progression_updated_at,
            "product_count": portfolio.get("total") or 0,
            "action_count": actions.get("total") or 0,
            "progression_count": progression.get("total") or 0,
        },
        "sources": [
            {"kind": "dev_product_portfolio", "object": portfolio.get("object")},
            {"kind": "dev_product_actions", "object": actions.get("object")},
            {"kind": "dev_product_progression_loop", "object": progression.get("object")},
        ],
    }


def build_product_live_snapshot(
    *,
    store: DevProductStore,
    product: Dict[str, Any],
    execution_store: Any,
    clarification_store: Any = None,
    plan_artifact_store: Any = None,
    goal_store: Any = None,
    verification_store: Any = None,
    incident_store: Any = None,
    event_store: Any = None,
    action_limit: int = 20,
    plan_limit: int = 100,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build one read-only selected Product snapshot for live console streams."""

    emitted_at = float(now or time.time())
    product_id = str(product.get("product_id") or "").strip()
    try:
        backlog = build_product_backlog(
            product=product,
            execution_store=execution_store,
            goal_store=goal_store,
            verification_store=verification_store,
            incident_store=incident_store,
            event_store=event_store,
            plan_limit=plan_limit,
            now=emitted_at,
        )
    except Exception as exc:
        backlog = _unknown_backlog_for_product(product, updated_at=emitted_at, reason=str(exc))
    actions = build_product_action_surface(
        store=store,
        execution_store=execution_store,
        clarification_store=clarification_store,
        plan_artifact_store=plan_artifact_store,
        goal_store=goal_store,
        verification_store=verification_store,
        incident_store=incident_store,
        event_store=event_store,
        product_id=product_id,
        action_limit=action_limit,
        now=emitted_at,
    )
    progression = list_product_progression_loop(
        store=store,
        product_id=product_id,
        limit=1,
        latest_only=True,
        now=emitted_at,
    )
    progression_updated_at = max(
        [_float_or_zero(item.get("evaluated_at")) for item in progression.get("data") or []] + [0.0],
    ) or None
    return {
        "object": "hermes.dev_product_live_snapshot",
        "event": "dev.product.snapshot",
        "emitted_at": emitted_at,
        "product": product,
        "backlog": backlog,
        "actions": actions,
        "progression_loop": progression,
        "freshness": {
            "emitted_at": emitted_at,
            "product_updated_at": product.get("updated_at"),
            "backlog_updated_at": backlog.get("updated_at"),
            "actions_updated_at": actions.get("updated_at"),
            "progression_updated_at": progression_updated_at,
            "backlog_count": (backlog.get("counts") or {}).get("total") or 0,
            "action_count": actions.get("total") or 0,
            "progression_count": progression.get("total") or 0,
        },
        "sources": [
            {"kind": "dev_product", "object": product.get("object")},
            {"kind": "dev_product_backlog", "object": backlog.get("object")},
            {"kind": "dev_product_actions", "object": actions.get("object")},
            {"kind": "dev_product_progression_loop", "object": progression.get("object")},
        ],
    }


def tick_product_progression_loop(
    *,
    store: DevProductStore,
    execution_store: Any,
    clarification_store: Any = None,
    plan_artifact_store: Any = None,
    goal_store: Any = None,
    verification_store: Any = None,
    incident_store: Any = None,
    event_store: Any = None,
    bridge: Any = None,
    product_id: Optional[str] = None,
    product_limit: int = 25,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Record bounded advisory Product progression loop iterations."""

    evaluated_at = float(now or time.time())
    if product_id:
        product = store.get(product_id)
        products = [product] if product else []
    else:
        products = store.list(include_archived=False, limit=max(1, min(int(product_limit or 25), 100)))
    portfolio_flow_control = store.get_portfolio_flow_control()

    iterations: list[Dict[str, Any]] = []
    for product in products:
        try:
            backlog = build_product_backlog(
                product=product,
                execution_store=execution_store,
                goal_store=goal_store,
                verification_store=verification_store,
                incident_store=incident_store,
                event_store=event_store,
                now=evaluated_at,
            )
        except Exception as exc:
            backlog = _unknown_backlog_for_product(product, updated_at=evaluated_at, reason=str(exc))
        action_surface = build_product_action_surface_for_product(
            product=product,
            backlog=backlog,
            execution_store=execution_store,
            clarification_store=clarification_store,
            plan_artifact_store=plan_artifact_store,
            now=evaluated_at,
            action_limit=20,
        )
        supervisor_state = _supervisor_loop_state_for_product(product, execution_store)
        iteration = _product_progression_iteration(
            product=product,
            backlog=backlog,
            action_surface=action_surface,
            supervisor_state=supervisor_state,
            portfolio_flow_control=portfolio_flow_control,
            evaluated_at=evaluated_at,
        )
        _apply_bounded_progression_transition(
            iteration=iteration,
            backlog=backlog,
            execution_store=execution_store,
            verification_store=verification_store,
            bridge=bridge,
            event_store=event_store,
        )
        iterations.append(store.record_progression_iteration(iteration))

    iterations.sort(key=_product_progression_sort_key)
    for index, item in enumerate(iterations, start=1):
        item["ordering"] = index
    return {
        "ok": True,
        "object": "hermes.dev_product_progression_loop_tick",
        "status": "completed",
        "evaluated_at": evaluated_at,
        "evaluated_count": len(iterations),
        "counts": _product_progression_counts(iterations),
        "data": iterations,
    }


def _product_progression_iteration(
    *,
    product: Dict[str, Any],
    backlog: Dict[str, Any],
    action_surface: Dict[str, Any],
    supervisor_state: Optional[Dict[str, Any]],
    portfolio_flow_control: Optional[Dict[str, Any]] = None,
    evaluated_at: float,
) -> Dict[str, Any]:
    product_id = str(product.get("product_id") or "").strip()
    project_id = resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID)
    flow_control = product_flow_control(product)
    flow_state = normalize_flow_control_state(flow_control.get("state"), default="normal")
    portfolio_flow_control = normalize_flow_control(portfolio_flow_control, scope="portfolio")
    portfolio_flow_state = normalize_flow_control_state(portfolio_flow_control.get("state"), default="normal")
    product_autonomy_level = normalize_autonomy_level(flow_control.get("autonomy_level"), default=DEFAULT_PRODUCT_AUTONOMY_LEVEL)
    portfolio_autonomy_level = normalize_autonomy_level(portfolio_flow_control.get("autonomy_level"), default=DEFAULT_PRODUCT_AUTONOMY_LEVEL)
    effective_autonomy_level = _effective_autonomy_level(product_autonomy_level, portfolio_autonomy_level)
    actions = list(action_surface.get("actions") or [])
    selected_action = _select_progression_action(actions)
    selected_backlog_item = _select_progression_backlog_item(backlog)
    counts = backlog.get("counts") if isinstance(backlog.get("counts"), dict) else _backlog_counts([])
    source_refs = _progression_source_refs(
        product=product,
        flow_control=flow_control,
        portfolio_flow_control=portfolio_flow_control,
        selected_action=selected_action,
        selected_backlog_item=selected_backlog_item,
        supervisor_state=supervisor_state,
    )

    if portfolio_flow_state != "normal":
        status = "held_by_flow_control"
        reason = portfolio_flow_control.get("reason") or f"Portfolio flow-control state is {portfolio_flow_state}."
        decision = "held_by_portfolio_flow_control"
    elif flow_state != "normal":
        status = "held_by_flow_control"
        reason = flow_control.get("reason") or f"Product flow-control state is {flow_state}."
        decision = "held_by_product_flow_control"
        if selected_action is None:
            selected_action = next(
                (
                    action
                    for action in actions
                    if (action.get("source") or {}).get("kind") == "dev_product_flow_control"
                ),
                None,
            )
    elif effective_autonomy_level == "manual":
        status = "waiting_for_human"
        reason = "Manual autonomy requires Felipe to initiate Product progression."
        decision = "manual_waiting_for_human"
    elif selected_action and selected_action.get("priority") in {"critical", "high"}:
        status = "waiting_for_human"
        reason = selected_action.get("reason") or selected_action.get("title") or "Product action requires human input."
        decision = "human_action_required"
    elif selected_backlog_item and selected_backlog_item.get("state") in PRODUCT_ACTION_ATTENTION_STATES:
        if effective_autonomy_level == "bounded" and _backlog_item_is_verification_ready(selected_backlog_item):
            status = "advanced"
            reason = "Product has completed or review-ready execution work available for acceptance verification."
            decision = "advisory_verification_advance"
        else:
            status = "blocked"
            reason = selected_backlog_item.get("blocking_reason") or "Product backlog needs attention."
            decision = "blocked_by_backlog_attention"
    elif int(counts.get("unknown") or 0) > 0:
        status = "blocked"
        reason = "Product has backlog work with unknown execution state."
        decision = "blocked_by_unknown_backlog_state"
    elif int(counts.get("in_flight") or 0) > 0 or int(counts.get("planned") or 0) > 0:
        status = "advanced"
        reason = "Product has active or planned execution-derived work available for continued Dev supervision."
        decision = "advisory_advance"
    elif _select_progression_verification_candidate(backlog, verification_store=None):
        status = "advanced"
        reason = "Product has completed execution work available for acceptance verification."
        decision = "advisory_verification_advance"
    elif normalize_lifecycle_state(product.get("lifecycle_state")) in {"active", "shipping", "planned", "framing"}:
        status = "advanced"
        reason = "Product lifecycle indicates active or planned work; no blocking Product action was selected."
        decision = "advisory_lifecycle_advance"
    else:
        status = "idle"
        reason = "No current Product work requires autonomous progression."
        decision = "idle"

    iteration_id = _stable_progression_iteration_id(product_id=product_id, evaluated_at=evaluated_at)
    selected_action_id = selected_action.get("id") if selected_action else None
    selected_action_kind = selected_action.get("kind") if selected_action else None
    return {
        "iteration_id": iteration_id,
        "object": "hermes.dev_product_progression_iteration",
        "product_id": product_id,
        "project_id": project_id,
        "status": status,
        "reason": reason,
        "selected_action_id": selected_action_id,
        "selected_action_kind": selected_action_kind,
        "selected_action": _compact_progression_action(selected_action),
        "source_refs": source_refs,
        "portfolio_flow_control": portfolio_flow_control,
        "flow_control": flow_control,
        "autonomy_policy": {
            "product_autonomy_level": product_autonomy_level,
            "portfolio_autonomy_level": portfolio_autonomy_level,
            "effective_autonomy_level": effective_autonomy_level,
            "decision": decision,
            "reason": reason,
        },
        "backlog_counts": counts,
        "action_counts": action_surface.get("counts") or _product_action_counts(actions),
        "supervisor_loop": _compact_supervisor_loop_state(supervisor_state),
        "evaluated_at": evaluated_at,
        "created_at": evaluated_at,
    }


def _effective_autonomy_level(product_level: Any, portfolio_level: Any) -> str:
    rank = {
        "manual": 0,
        "supervised": 1,
        "bounded": 2,
    }
    product = normalize_autonomy_level(product_level, default=DEFAULT_PRODUCT_AUTONOMY_LEVEL)
    portfolio = normalize_autonomy_level(portfolio_level, default=DEFAULT_PRODUCT_AUTONOMY_LEVEL)
    if product not in rank:
        product = DEFAULT_PRODUCT_AUTONOMY_LEVEL
    if portfolio not in rank:
        portfolio = DEFAULT_PRODUCT_AUTONOMY_LEVEL
    return product if rank[product] <= rank[portfolio] else portfolio


def _apply_bounded_progression_transition(
    *,
    iteration: Dict[str, Any],
    backlog: Dict[str, Any],
    execution_store: Any,
    verification_store: Any = None,
    bridge: Any = None,
    event_store: Any = None,
) -> None:
    policy = iteration.get("autonomy_policy") if isinstance(iteration.get("autonomy_policy"), dict) else {}
    if iteration.get("status") != "advanced" or policy.get("effective_autonomy_level") != "bounded":
        return
    if policy.get("decision") not in {"advisory_advance", "advisory_lifecycle_advance", "advisory_verification_advance"}:
        return
    if _apply_bounded_verification_transition(
        iteration=iteration,
        backlog=backlog,
        execution_store=execution_store,
        verification_store=verification_store,
        bridge=bridge,
        event_store=event_store,
    ):
        return
    selected = _select_progression_backlog_item(backlog)
    if not selected or selected.get("state") != "planned":
        return
    source = selected.get("source") if isinstance(selected.get("source"), dict) else {}
    if source.get("kind") != "dev_execution_task":
        return
    plan_id = str(source.get("plan_id") or "").strip()
    task_id = str(source.get("task_id") or "").strip()
    if not plan_id or not task_id:
        return
    transition: Dict[str, Any] = {
        "object": "hermes.dev_product_progression_transition",
        "action": "launch_execution_task",
        "status": "attempted",
        "plan_id": plan_id,
        "task_id": task_id,
        "reason": "Bounded autonomy selected planned Product execution work for launch.",
    }
    try:
        from gateway.dev_execution import launch_execution_plan

        launch = launch_execution_plan(
            store=execution_store,
            plan_id=plan_id,
            task_ids=[task_id],
            bridge=bridge,
            event_store=event_store,
        )
        launched = launch.get("launched") if isinstance(launch.get("launched"), list) else []
        failures = launch.get("failures") if isinstance(launch.get("failures"), list) else []
        transition.update({
            "status": "applied" if launched else "failed",
            "reason": "Bounded autonomy launched planned Product execution work." if launched else "Bounded launch did not start worker work.",
            "launch": {
                "ok": bool(launch.get("ok")),
                "launched_count": len(launched),
                "failure_count": len(failures),
                "launched_task_ids": [item.get("task_id") for item in launched if item.get("task_id")],
                "failures": failures,
                "launch_id": (launch.get("launch_record") or {}).get("launch_id") if isinstance(launch.get("launch_record"), dict) else None,
            },
        })
        if launched:
            iteration["reason"] = "Bounded autonomy launched planned Product execution work."
            policy["decision"] = "bounded_launch_applied"
            policy["reason"] = iteration["reason"]
        else:
            iteration["status"] = "blocked"
            iteration["reason"] = "Bounded launch did not start worker work."
            policy["decision"] = "bounded_launch_failed"
            policy["reason"] = iteration["reason"]
    except Exception as exc:
        transition.update({
            "status": "failed",
            "reason": str(exc),
            "error": str(exc),
        })
        iteration["status"] = "blocked"
        iteration["reason"] = str(exc)
        policy["decision"] = "bounded_launch_failed"
        policy["reason"] = str(exc)
    iteration["autonomy_policy"] = policy
    iteration["transition"] = transition


def _apply_bounded_verification_transition(
    *,
    iteration: Dict[str, Any],
    backlog: Dict[str, Any],
    execution_store: Any,
    verification_store: Any = None,
    bridge: Any = None,
    event_store: Any = None,
) -> bool:
    if verification_store is None:
        return False
    selected = _select_progression_verification_candidate(backlog, verification_store=verification_store)
    if not selected:
        return False
    source = selected.get("source") if isinstance(selected.get("source"), dict) else {}
    plan_id = str(source.get("plan_id") or "").strip()
    task_id = str(source.get("task_id") or "").strip()
    if not plan_id or not task_id:
        return False
    policy = iteration.get("autonomy_policy") if isinstance(iteration.get("autonomy_policy"), dict) else {}
    transition: Dict[str, Any] = {
        "object": "hermes.dev_product_progression_transition",
        "action": "launch_acceptance_verification",
        "status": "attempted",
        "plan_id": plan_id,
        "task_id": task_id,
        "reason": "Bounded autonomy selected completed Product execution work for acceptance verification.",
    }
    try:
        from gateway.dev_control.acceptance_verification import launch_verification_run

        run = launch_verification_run(
            execution_store=execution_store,
            verification_store=verification_store,
            plan_id=plan_id,
            task_id=task_id,
            bridge=bridge,
            event_store=event_store,
        )
        run_status = str(run.get("status") or "unknown").strip().lower()
        transition.update({
            "status": "skipped" if run_status == "skipped" else "applied",
            "reason": (
                "Bounded autonomy recorded skipped acceptance verification."
                if run_status == "skipped"
                else "Bounded autonomy launched acceptance verification."
            ),
            "verification": {
                "verification_run_id": run.get("verification_run_id"),
                "status": run.get("status"),
                "verdict": run.get("verdict"),
                "acceptance_verification_score": run.get("acceptance_verification_score"),
                "verification_session_id": run.get("verification_session_id"),
                "verification_runtime": run.get("verification_runtime"),
                "warnings": run.get("warnings") or [],
            },
        })
        if run_status == "skipped":
            iteration["reason"] = "Bounded autonomy recorded skipped acceptance verification."
            policy["decision"] = "bounded_verification_skipped"
        else:
            iteration["reason"] = "Bounded autonomy launched acceptance verification."
            policy["decision"] = "bounded_verification_applied"
        policy["reason"] = iteration["reason"]
    except Exception as exc:
        transition.update({
            "status": "failed",
            "reason": str(exc),
            "error": str(exc),
        })
        iteration["status"] = "blocked"
        iteration["reason"] = str(exc)
        policy["decision"] = "bounded_verification_failed"
        policy["reason"] = str(exc)
    iteration["autonomy_policy"] = policy
    iteration["transition"] = transition
    return True


def _select_progression_action(actions: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for action in sorted(actions, key=_product_action_sort_key):
        if action.get("kind") == "backlog_attention":
            continue
        if action.get("priority") in {"critical", "high"}:
            return action
    return None


def _select_progression_backlog_item(backlog: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    items = backlog.get("items") if isinstance(backlog.get("items"), list) else []
    for state in ("failed", "blocked", "needs_attention", "unknown", "in_flight", "planned"):
        item = next((candidate for candidate in items if candidate.get("state") == state), None)
        if item:
            return item
    return None


def _select_progression_verification_candidate(backlog: Dict[str, Any], *, verification_store: Any = None) -> Optional[Dict[str, Any]]:
    items = backlog.get("items") if isinstance(backlog.get("items"), list) else []
    candidates = [item for item in items if _backlog_item_is_verification_ready(item)]
    candidates.sort(key=lambda item: (int(item.get("ordering") or 0), str(item.get("id") or "")))
    latest_for_task = getattr(verification_store, "latest_for_task", None) if verification_store is not None else None
    for item in candidates:
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        plan_id = str(source.get("plan_id") or "").strip()
        task_id = str(source.get("task_id") or "").strip()
        if not plan_id or not task_id:
            continue
        if callable(latest_for_task):
            try:
                if latest_for_task(plan_id=plan_id, task_id=task_id):
                    continue
            except Exception:
                continue
        return item
    return None


def _backlog_item_is_verification_ready(item: Dict[str, Any]) -> bool:
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    if source.get("kind") != "dev_execution_task":
        return False
    state = str(item.get("state") or "").strip().lower()
    source_status = str(item.get("source_status") or "").strip().lower()
    return state == "complete" or source_status in {"done", "merged", "completed", "complete", "success", "succeeded", "needs_review"}


def _progression_source_refs(
    *,
    product: Dict[str, Any],
    flow_control: Dict[str, Any],
    portfolio_flow_control: Optional[Dict[str, Any]],
    selected_action: Optional[Dict[str, Any]],
    selected_backlog_item: Optional[Dict[str, Any]],
    supervisor_state: Optional[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    refs: list[Dict[str, Any]] = [{
        "kind": "dev_product",
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
    }]
    refs.append({
        "kind": "dev_product_flow_control",
        "state": flow_control.get("state"),
        "autonomy_level": flow_control.get("autonomy_level"),
        "updated_at": flow_control.get("updated_at"),
    })
    if portfolio_flow_control:
        refs.append({
            "kind": "dev_portfolio_flow_control",
            "state": portfolio_flow_control.get("state"),
            "autonomy_level": portfolio_flow_control.get("autonomy_level"),
            "updated_at": portfolio_flow_control.get("updated_at"),
        })
    if selected_action:
        refs.append({
            "kind": "dev_product_action",
            "action_id": selected_action.get("id"),
            "action_kind": selected_action.get("kind"),
            "priority": selected_action.get("priority"),
        })
    if selected_backlog_item:
        source = selected_backlog_item.get("source") if isinstance(selected_backlog_item.get("source"), dict) else {}
        refs.append({
            "kind": "dev_product_backlog_item",
            "backlog_item_id": selected_backlog_item.get("id"),
            "state": selected_backlog_item.get("state"),
            **source,
        })
    if supervisor_state:
        refs.append({
            "kind": "dev_supervisor_loop",
            "project_id": supervisor_state.get("project_id"),
            "status": supervisor_state.get("status"),
            "last_run_id": supervisor_state.get("last_run_id"),
            "last_tick_at": supervisor_state.get("last_tick_at"),
        })
    return refs


def _supervisor_loop_state_for_product(product: Dict[str, Any], execution_store: Any) -> Optional[Dict[str, Any]]:
    if execution_store is None:
        return None
    get_state = getattr(execution_store, "get_supervisor_loop_state", None)
    if not callable(get_state):
        return None
    try:
        return get_state(resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID))
    except Exception:
        return None


def _compact_progression_action(action: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not action:
        return None
    return {
        "id": action.get("id"),
        "kind": action.get("kind"),
        "title": action.get("title"),
        "priority": action.get("priority"),
        "reason": action.get("reason"),
        "source": action.get("source") or {"kind": "unknown"},
        "updated_at": action.get("updated_at"),
    }


def _compact_supervisor_loop_state(state: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not state:
        return None
    return {
        "project_id": state.get("project_id"),
        "runbook_id": state.get("runbook_id"),
        "status": state.get("status"),
        "last_run_id": state.get("last_run_id"),
        "last_tick_at": state.get("last_tick_at"),
        "next_tick_at": state.get("next_tick_at"),
        "last_message": state.get("last_message"),
        "consecutive_error_count": state.get("consecutive_error_count") or 0,
    }


def _product_progression_counts(iterations: list[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "total": len(iterations),
        "advanced": 0,
        "blocked": 0,
        "waiting_for_human": 0,
        "held_by_flow_control": 0,
        "idle": 0,
        "unknown": 0,
    }
    for iteration in iterations:
        status = str(iteration.get("status") or "unknown")
        counts[status if status in counts else "unknown"] += 1
    return counts


def _product_progression_sort_key(iteration: Dict[str, Any]) -> tuple[int, float, str, str]:
    rank = {
        "waiting_for_human": 0,
        "held_by_flow_control": 1,
        "blocked": 2,
        "advanced": 3,
        "idle": 4,
    }.get(str(iteration.get("status") or "unknown"), 5)
    return (
        rank,
        -_float_or_zero(iteration.get("evaluated_at")),
        str(iteration.get("project_id") or ""),
        str(iteration.get("product_id") or ""),
    )


def _stable_progression_iteration_id(*, product_id: str, evaluated_at: float) -> str:
    digest = hashlib.blake2s(f"{product_id}:{evaluated_at:.6f}".encode("utf-8"), digest_size=8).hexdigest()
    return f"product-loop-{digest}"


def _backlog_item_from_task(
    *,
    product: Dict[str, Any],
    plan: Dict[str, Any],
    task: Dict[str, Any],
    plan_index: int,
    task_index: int,
    goals_by_id: Dict[str, Dict[str, Any]],
    milestone_by_id: Dict[str, Dict[str, Any]],
    launch_records: list[Dict[str, Any]],
    verification_store: Any,
    incident_records: list[Dict[str, Any]],
    event_store: Any,
    manual_context: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    plan_id = str(plan.get("plan_id") or "").strip()
    task_id = str(task.get("task_id") or "").strip()
    state, blocking_reason = _backlog_state_from_task(task)
    linked_goal_id = _linked_goal_id(task)
    milestone_id = _milestone_id_for_goal(
        linked_goal_id,
        goals_by_id=goals_by_id,
        milestone_by_id=milestone_by_id,
    ) or _milestone_id_from_task(task, milestone_by_id)
    evidence_links = [
        {"kind": "dev_execution_plan", "plan_id": plan_id, "status": plan.get("status")},
        {"kind": "dev_execution_task", "plan_id": plan_id, "task_id": task_id, "status": task.get("status")},
    ]
    ao_session_id = str(task.get("ao_session_id") or "").strip()
    if ao_session_id:
        evidence_links.append({"kind": "worker_session", "session_id": ao_session_id})
    evidence_links.extend(_launch_evidence_links(launch_records, task_id))
    evidence_links.extend(_verification_evidence_links(verification_store, plan_id, task_id))
    evidence_links.extend(_event_evidence_links(event_store, ao_session_id))
    incident_links = _incident_evidence_links(
        incident_records,
        project_id=product.get("project_id"),
        plan_id=plan_id,
        task_id=task_id,
        ao_session_id=ao_session_id,
    )
    evidence_links.extend(incident_links)
    if incident_links and state not in BACKLOG_TERMINAL_STATES:
        state = "blocked"
        blocking_reason = "Unresolved incident is linked to this work."
    latest_source_at = max(
        [
            _float_or_zero(task.get("updated_at")),
            _float_or_zero(plan.get("updated_at")),
            *[_float_or_zero(link.get("updated_at") or link.get("created_at") or link.get("detected_at")) for link in evidence_links],
        ],
    )
    source_key = (plan_id, task_id)
    item = {
        "id": f"backlog-task-{plan_id}-{task_id}",
        "object": "hermes.dev_product_backlog_item",
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
        "title": task.get("goal") or task.get("prompt") or task_id,
        "state": state,
        "source_status": task.get("status"),
        "source": {"kind": "dev_execution_task", "plan_id": plan_id, "task_id": task_id},
        "linked_goal_id": linked_goal_id,
        "milestone_id": milestone_id,
        "blocking_reason": blocking_reason,
        "evidence_links": evidence_links,
        "manual_context": manual_context.get(source_key) or (manual_context.get((linked_goal_id, "")) if linked_goal_id else None),
        "freshness": {"latest_source_at": latest_source_at or None},
        "updated_at": latest_source_at or task.get("updated_at") or plan.get("updated_at"),
        "ordering": (plan_index + 1) * 1000 + task_index + 1,
    }
    return item


def _backlog_item_from_goal(
    *,
    product: Dict[str, Any],
    goal: Dict[str, Any],
    goals_by_id: Dict[str, Dict[str, Any]],
    milestone_by_id: Dict[str, Dict[str, Any]],
    manual_context: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    goal_id = str(goal.get("goal_id") or "").strip()
    state, blocking_reason = _backlog_state_from_goal(goal)
    milestone_id = _milestone_id_for_goal(
        goal_id,
        goals_by_id=goals_by_id,
        milestone_by_id=milestone_by_id,
    )
    updated_at = _float_or_zero(goal.get("updated_at")) or _float_or_zero(goal.get("created_at"))
    return {
        "id": f"backlog-goal-{goal_id}",
        "object": "hermes.dev_product_backlog_item",
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
        "title": goal.get("title") or goal_id,
        "state": state,
        "source_status": goal.get("status"),
        "source": {"kind": "dev_project_goal", "goal_id": goal_id},
        "linked_goal_id": goal_id,
        "milestone_id": milestone_id,
        "blocking_reason": blocking_reason,
        "evidence_links": [{"kind": "dev_project_goal", "goal_id": goal_id, "status": goal.get("status")}],
        "manual_context": manual_context.get((goal_id, "")),
        "freshness": {"latest_source_at": updated_at or None},
        "updated_at": updated_at or None,
        "ordering": int(goal.get("ordering") or 0) + 500000,
    }


def _backlog_state_from_task(task: Dict[str, Any]) -> tuple[str, Optional[str]]:
    status = str(task.get("status") or "").strip().lower()
    if not status:
        return "unknown", "Task state could not be derived from Hermes execution data."
    if status in {"planned", "draft", "ready"}:
        return "planned", None
    if status in {"queued", "spawning", "launched", "working", "running", "active", "started"}:
        return "in_flight", None
    if status in {"done", "merged", "completed", "complete", "success", "succeeded"}:
        return "complete", None
    if status in {"needs_review", "needs_attention", "needs_input", "approval_required", "blocked", "paused"}:
        return "needs_attention", "Task is waiting for input, approval, review, or unblock."
    if status in {"killed", "errored", "error", "terminated", "failed", "cancelled", "canceled"}:
        return "failed", "Task ended with failed status."
    return "unknown", f"Unrecognized Hermes task status: {status}."


def _backlog_state_from_goal(goal: Dict[str, Any]) -> tuple[str, Optional[str]]:
    status = str(goal.get("status") or "").strip().lower()
    if status in {"proposed", "active"}:
        return "planned", None
    if status == "blocked":
        return "blocked", "Goal is blocked."
    if status == "achieved":
        return "complete", None
    if status == "abandoned":
        return "failed", "Goal was abandoned."
    return "unknown", "Goal state could not be derived from Hermes project-goal data."


def _list_execution_plans(execution_store: Any, *, project_id: str, limit: int) -> list[Dict[str, Any]]:
    if execution_store is None:
        return []
    try:
        return list(execution_store.list_plans(limit=limit, project_id=project_id) or [])
    except Exception:
        return []


def _list_launch_records(execution_store: Any, plan_id: Any) -> list[Dict[str, Any]]:
    if execution_store is None or not plan_id:
        return []
    list_records = getattr(execution_store, "list_launch_records", None)
    if not callable(list_records):
        return []
    try:
        return list(list_records(str(plan_id), limit=20) or [])
    except Exception:
        return []


def _list_goals(goal_store: Any, *, project_id: str) -> list[Dict[str, Any]]:
    if goal_store is None:
        return []
    list_goals = getattr(goal_store, "list", None)
    if not callable(list_goals):
        return []
    try:
        return list(list_goals(project_id=project_id, include_abandoned=True, limit=500) or [])
    except Exception:
        return []


def _list_incidents(incident_store: Any) -> list[Dict[str, Any]]:
    if incident_store is None:
        return []
    list_incidents = getattr(incident_store, "list_incidents", None)
    if not callable(list_incidents):
        return []
    try:
        return list(list_incidents(limit=200) or [])
    except Exception:
        return []


def _linked_goal_id(task: Dict[str, Any]) -> Optional[str]:
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    for key in ("linked_goal_id", "goal_id", "subgoal_id"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return None


def _milestone_id_from_task(task: Dict[str, Any], milestone_by_id: Dict[str, Dict[str, Any]]) -> Optional[str]:
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    value = str(payload.get("milestone_goal_id") or payload.get("milestone_id") or "").strip()
    return value if value in milestone_by_id else None


def _milestone_id_for_goal(
    goal_id: Optional[str],
    *,
    goals_by_id: Dict[str, Dict[str, Any]],
    milestone_by_id: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    current_id = str(goal_id or "").strip()
    visited: set[str] = set()
    while current_id and current_id not in visited:
        visited.add(current_id)
        current = goals_by_id.get(current_id)
        if not current:
            return None
        if current.get("kind") == "milestone" and current_id in milestone_by_id:
            return current_id
        current_id = str(current.get("parent_goal_id") or "").strip()
    return None


def _launch_evidence_links(launch_records: list[Dict[str, Any]], task_id: str) -> list[Dict[str, Any]]:
    links: list[Dict[str, Any]] = []
    for record in launch_records:
        launched = set(str(value) for value in (record.get("launched_task_ids") or []))
        failed = set(str(value) for value in (record.get("failed_task_ids") or []))
        requested = set(str(value) for value in (record.get("requested_task_ids") or []))
        if task_id in launched or task_id in failed or task_id in requested:
            links.append({
                "kind": "dev_execution_launch",
                "launch_id": record.get("launch_id"),
                "plan_id": record.get("plan_id"),
                "status": record.get("status"),
                "created_at": record.get("created_at"),
            })
    return links


def _verification_evidence_links(verification_store: Any, plan_id: str, task_id: str) -> list[Dict[str, Any]]:
    if verification_store is None:
        return []
    latest_for_task = getattr(verification_store, "latest_for_task", None)
    if not callable(latest_for_task):
        return []
    try:
        run = latest_for_task(plan_id=plan_id, task_id=task_id)
    except Exception:
        return []
    if not run:
        return []
    return [{
        "kind": "dev_verification_run",
        "verification_run_id": run.get("verification_run_id"),
        "status": run.get("status"),
        "verdict": run.get("verdict"),
        "created_at": run.get("created_at"),
        "updated_at": run.get("updated_at"),
    }]


def _event_evidence_links(event_store: Any, ao_session_id: str) -> list[Dict[str, Any]]:
    if event_store is None or not ao_session_id:
        return []
    list_events = getattr(event_store, "list_events", None)
    if not callable(list_events):
        return []
    try:
        events = list_events(ao_session_id=ao_session_id, limit=200)
    except Exception:
        return []
    if not events:
        return []
    latest = events[-1]
    return [{
        "kind": "subagent_event",
        "event_id": latest.get("event_id"),
        "event": latest.get("event"),
        "status": latest.get("status"),
        "created_at": latest.get("created_at"),
    }]


def _incident_evidence_links(
    incident_records: list[Dict[str, Any]],
    *,
    project_id: Any,
    plan_id: str,
    task_id: str,
    ao_session_id: str,
) -> list[Dict[str, Any]]:
    needles = {str(value).strip() for value in (project_id, plan_id, task_id, ao_session_id) if str(value or "").strip()}
    links: list[Dict[str, Any]] = []
    for incident in incident_records:
        status = str(incident.get("status") or "").strip().lower()
        if status == "resolved":
            continue
        haystack = json.dumps({
            "evidence_refs": incident.get("evidence_refs"),
            "correlated_release": incident.get("correlated_release"),
            "clusters": incident.get("clusters"),
        }, ensure_ascii=False)
        if any(needle and needle in haystack for needle in needles):
            links.append({
                "kind": "dev_incident",
                "incident_id": incident.get("incident_id"),
                "status": incident.get("status"),
                "severity": incident.get("severity"),
                "detected_at": incident.get("detected_at"),
            })
    return links


def _milestone_groups(
    items: list[Dict[str, Any]],
    milestone_by_id: Dict[str, Dict[str, Any]],
) -> list[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for item in items:
        milestone_id = item.get("milestone_id") or "unmilestoned"
        milestone = milestone_by_id.get(str(milestone_id)) or {}
        group = grouped.setdefault(str(milestone_id), {
            "milestone_id": None if milestone_id == "unmilestoned" else milestone_id,
            "title": milestone.get("title") or "Unmilestoned",
            "status": milestone.get("status") or "unknown",
            "ordering": int(milestone.get("ordering") or 999999),
            "item_ids": [],
            "counts": {},
        })
        group["item_ids"].append(item["id"])
    for group in grouped.values():
        group["counts"] = _backlog_counts([item for item in items if item["id"] in set(group["item_ids"])])
    return sorted(grouped.values(), key=lambda group: (group["ordering"], group["title"]))


def _backlog_counts(items: list[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "total": len(items),
        "planned": 0,
        "in_flight": 0,
        "needs_attention": 0,
        "blocked": 0,
        "failed": 0,
        "complete": 0,
        "unknown": 0,
    }
    for item in items:
        state = str(item.get("state") or "unknown")
        counts[state if state in counts else "unknown"] += 1
    return counts


def _next_actionable_item_id(items: list[Dict[str, Any]]) -> Optional[str]:
    for item in items:
        if item.get("state") in {"planned", "in_flight", "needs_attention", "blocked", "failed", "unknown"}:
            return item.get("id")
    return None


def _unknown_backlog_for_product(product: Dict[str, Any], *, updated_at: float, reason: str) -> Dict[str, Any]:
    return {
        "object": "hermes.dev_product_backlog",
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
        "flow_control": product_flow_control(product),
        "updated_at": updated_at,
        "freshness": {
            "updated_at": updated_at,
            "latest_source_at": None,
            "source_count": 0,
            "state": "unknown",
            "reason": reason or "Product backlog unavailable.",
        },
        "counts": _backlog_counts([]),
        "next_item_id": None,
        "milestone_groups": [],
        "items": [],
    }


def _flow_control_actions(product: Dict[str, Any], *, updated_at: float) -> list[Dict[str, Any]]:
    flow_control = product_flow_control(product)
    state = normalize_flow_control_state(flow_control.get("state"), default="normal")
    if state == "normal":
        return []
    if state == "needs_direction":
        kind = "direction_needed"
        title = "Product direction needed"
        priority = "critical"
    elif state == "paused":
        kind = "product_paused"
        title = "Product flow paused"
        priority = "medium"
    elif state == "hold_new_work":
        kind = "hold_new_work"
        title = "Product is holding new work"
        priority = "medium"
    else:
        kind = "flow_unknown"
        title = "Product flow-control state unknown"
        priority = "high"
    return [_product_action(
        product=product,
        action_id=f"action-flow-{product.get('product_id')}-{state}",
        kind=kind,
        title=title,
        priority=priority,
        reason=flow_control.get("reason"),
        source={"kind": "dev_product_flow_control", "state": state},
        updated_at=flow_control.get("updated_at") or updated_at,
    )]


def _clarification_actions(product: Dict[str, Any], clarification_store: Any) -> list[Dict[str, Any]]:
    if clarification_store is None:
        return []
    project_id = resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID)
    actions: list[Dict[str, Any]] = []
    for status in ("brief_ready", "active"):
        list_items = getattr(clarification_store, "list", None)
        if not callable(list_items):
            continue
        try:
            items = list_items(project_id=project_id, status=status, limit=50) or []
        except Exception:
            items = []
        for item in items:
            clarification_id = str(item.get("clarification_id") or "").strip()
            if not clarification_id:
                continue
            actions.append(_product_action(
                product=product,
                action_id=f"action-clarification-{clarification_id}",
                kind="clarification",
                title=item.get("title") or ("Discovery brief ready" if status == "brief_ready" else "Clarification needs input"),
                priority="critical" if status == "brief_ready" else "high",
                reason=item.get("current_question", {}).get("prompt") if isinstance(item.get("current_question"), dict) else None,
                source={"kind": "dev_clarification", "clarification_id": clarification_id, "status": status},
                updated_at=item.get("updated_at") or item.get("created_at"),
            ))
    return actions


def _draft_review_actions(product: Dict[str, Any], execution_store: Any) -> list[Dict[str, Any]]:
    if execution_store is None:
        return []
    project_id = resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID)
    actions: list[Dict[str, Any]] = []
    for plan in _list_execution_plans(execution_store, project_id=project_id, limit=100):
        get_review = getattr(execution_store, "get_draft_review", None)
        if not callable(get_review):
            continue
        try:
            review = get_review(plan.get("plan_id"))
        except Exception:
            review = None
        if not review:
            continue
        draft_status = str(review.get("draft_status") or "").strip()
        if draft_status in {"approved_for_launch", "cancelled"}:
            continue
        actions.append(_product_action(
            product=product,
            action_id=f"action-draft-review-{plan.get('plan_id')}",
            kind="draft_review",
            title=f"Review execution draft: {plan.get('title') or plan.get('plan_id')}",
            priority="high",
            reason=f"Draft status is {draft_status or 'unknown'}.",
            source={
                "kind": "dev_execution_plan_draft_review",
                "plan_id": plan.get("plan_id"),
                "plan_artifact_id": review.get("plan_artifact_id"),
                "draft_status": draft_status,
            },
            updated_at=review.get("updated_at") or plan.get("updated_at"),
        ))
    return actions


def _supervisor_approval_actions(product: Dict[str, Any], execution_store: Any) -> list[Dict[str, Any]]:
    if execution_store is None:
        return []
    project_id = resolve_project_id(product.get("project_id"), default=DEFAULT_PROJECT_ID)
    actions: list[Dict[str, Any]] = []
    list_approvals = getattr(execution_store, "list_supervisor_approvals", None)
    if not callable(list_approvals):
        return actions
    for status in ("approved", "pending"):
        try:
            approvals = list_approvals(status=status, limit=100) or []
        except Exception:
            approvals = []
        for approval in approvals:
            plan = execution_store.get_plan(approval.get("plan_id")) if hasattr(execution_store, "get_plan") else None
            if not plan or project_id not in _plan_project_ids(plan, default=project_id):
                continue
            approval_id = str(approval.get("approval_id") or "").strip()
            actions.append(_product_action(
                product=product,
                action_id=f"action-supervisor-approval-{approval_id}",
                kind="supervisor_approval",
                title=_supervisor_action_title(approval),
                priority="critical" if status == "pending" else "high",
                reason=approval.get("reason"),
                source={
                    "kind": "dev_supervisor_approval",
                    "approval_id": approval_id,
                    "plan_id": approval.get("plan_id"),
                    "status": status,
                    "recommended_action": approval.get("recommended_action"),
                },
                updated_at=approval.get("created_at"),
            ))
    return actions


def _backlog_attention_actions(product: Dict[str, Any], backlog: Dict[str, Any]) -> list[Dict[str, Any]]:
    items = backlog.get("items") if isinstance(backlog.get("items"), list) else []
    actions: list[Dict[str, Any]] = []
    for item in items:
        state = str(item.get("state") or "unknown")
        if state not in PRODUCT_ACTION_ATTENTION_STATES:
            continue
        actions.append(_product_action(
            product=product,
            action_id=f"action-backlog-{item.get('id')}",
            kind="backlog_attention",
            title=item.get("title") or item.get("id") or "Product backlog needs attention",
            priority="high" if state in {"failed", "blocked"} else "medium",
            reason=item.get("blocking_reason"),
            source={
                "kind": "dev_product_backlog_item",
                "backlog_item_id": item.get("id"),
                "state": state,
                **(item.get("source") if isinstance(item.get("source"), dict) else {}),
            },
            updated_at=item.get("updated_at") or (item.get("freshness") or {}).get("latest_source_at"),
        ))
    return actions


def _product_action(
    *,
    product: Dict[str, Any],
    action_id: str,
    kind: str,
    title: str,
    priority: str,
    reason: Optional[str],
    source: Dict[str, Any],
    updated_at: Any,
) -> Dict[str, Any]:
    return {
        "id": action_id,
        "object": "hermes.dev_product_action",
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
        "kind": kind,
        "title": title,
        "priority": priority if priority in {"critical", "high", "medium", "low", "unknown"} else "unknown",
        "reason": str(reason or "").strip() or None,
        "source": source or {"kind": "unknown"},
        "updated_at": _float_or_zero(updated_at) or None,
        "ordering": 0,
    }


def _product_action_counts(actions: list[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "total": len(actions),
        "critical": 0,
        "high": 0,
        "medium": 0,
        "low": 0,
        "unknown": 0,
    }
    for action in actions:
        priority = str(action.get("priority") or "unknown")
        counts[priority if priority in counts else "unknown"] += 1
    return counts


def _product_action_sort_key(action: Dict[str, Any]) -> tuple[int, float, str, str]:
    rank = {
        "critical": 0,
        "high": 1,
        "medium": 2,
        "low": 3,
        "unknown": 4,
    }.get(str(action.get("priority") or "unknown"), 4)
    updated_at = _float_or_zero(action.get("updated_at"))
    return (
        rank,
        updated_at or float("inf"),
        str(action.get("title") or "").lower(),
        str(action.get("id") or ""),
    )


def _supervisor_action_title(approval: Dict[str, Any]) -> str:
    action = str(approval.get("recommended_action") or "supervisor action").replace("_", " ")
    status = str(approval.get("status") or "pending")
    return f"{status.title()} supervisor action: {action}"


def _plan_project_ids(plan: Dict[str, Any], *, default: str) -> set[str]:
    project_ids = {resolve_project_id(plan.get("project_id"), default=default)}
    for task in plan.get("tasks") or []:
        if isinstance(task, dict):
            project_ids.add(resolve_project_id(task.get("project_id"), default=default))
    return project_ids


def _portfolio_item_from_product(
    product: Dict[str, Any],
    backlog: Dict[str, Any],
    *,
    action_surface: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    counts = backlog.get("counts") if isinstance(backlog.get("counts"), dict) else _backlog_counts([])
    next_item = _portfolio_next_item(backlog)
    flow_control = product_flow_control(product)
    action_counts = (action_surface or {}).get("counts") if isinstance((action_surface or {}).get("counts"), dict) else _product_action_counts([])
    next_action = (action_surface or {}).get("next_action") if isinstance((action_surface or {}).get("next_action"), dict) else None
    vision_alignment = _portfolio_vision_alignment(backlog)
    attention_state, attention_reason = _portfolio_attention(product, counts, next_item, flow_control)
    latest_source_at = _float_or_zero((backlog.get("freshness") or {}).get("latest_source_at"))
    updated_at = max(
        _float_or_zero(product.get("updated_at")),
        _float_or_zero(backlog.get("updated_at")),
        latest_source_at,
    ) or None
    return {
        "object": "hermes.dev_product_portfolio_item",
        "product": _compact_product(product),
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
        "name": product.get("name") or _display_name_from_project_id(product.get("project_id")),
        "lifecycle_state": normalize_lifecycle_state(product.get("lifecycle_state")),
        "flow_control": flow_control,
        "attention_state": attention_state,
        "attention_reason": attention_reason,
        "backlog_counts": counts,
        "next_item": next_item,
        "action_counts": action_counts,
        "next_action": next_action,
        "vision_alignment": vision_alignment,
        "freshness": _portfolio_freshness(backlog),
        "source": {
            "kind": "dev_product_backlog",
            "product_id": product.get("product_id"),
            "project_id": product.get("project_id"),
            "backlog_object": backlog.get("object"),
        },
        "updated_at": updated_at,
        "ordering": 0,
    }


def _compact_product(product: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "product_id": product.get("product_id"),
        "project_id": product.get("project_id"),
        "name": product.get("name") or _display_name_from_project_id(product.get("project_id")),
        "lifecycle_state": normalize_lifecycle_state(product.get("lifecycle_state")),
        "flow_control": product_flow_control(product),
        "primary_repo": product.get("primary_repo"),
        "updated_at": product.get("updated_at"),
    }


def _portfolio_next_item(backlog: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    next_item_id = backlog.get("next_item_id")
    items = backlog.get("items") if isinstance(backlog.get("items"), list) else []
    next_item = next((item for item in items if item.get("id") == next_item_id), None)
    if not next_item and items:
        next_item = items[0]
    if not next_item:
        return None
    return {
        "id": next_item.get("id"),
        "title": next_item.get("title"),
        "state": next_item.get("state") or "unknown",
        "source": next_item.get("source") or {"kind": "unknown"},
        "blocking_reason": next_item.get("blocking_reason"),
        "updated_at": next_item.get("updated_at"),
    }


def _portfolio_attention(
    product: Dict[str, Any],
    counts: Dict[str, int],
    next_item: Optional[Dict[str, Any]],
    flow_control: Optional[Dict[str, Any]] = None,
) -> tuple[str, Optional[str]]:
    flow_state = normalize_flow_control_state((flow_control or {}).get("state"), default="normal")
    if flow_state == "needs_direction":
        return "needs_attention", "Product flow is waiting for human product direction."
    if flow_state == "paused":
        return "planned", "Product flow is paused."
    if flow_state == "hold_new_work":
        return "planned", "Product flow is holding new work."
    lifecycle = normalize_lifecycle_state(product.get("lifecycle_state"))
    if int(counts.get("failed") or 0) > 0:
        return "needs_attention", "Failed backlog items need review."
    if int(counts.get("blocked") or 0) > 0 or lifecycle == "blocked":
        return "needs_attention", "Blocked Product work needs attention."
    if int(counts.get("needs_attention") or 0) > 0:
        return "needs_attention", "Product work is waiting for review, input, or approval."
    if int(counts.get("in_flight") or 0) > 0 or lifecycle in {"active", "shipping"}:
        return "active", None
    if int(counts.get("planned") or 0) > 0 or lifecycle in {"framing", "planned"}:
        return "planned", None
    if int(counts.get("total") or 0) > 0 and int(counts.get("complete") or 0) == int(counts.get("total") or 0):
        return "complete", None
    if lifecycle == "complete":
        return "complete", None
    if next_item and next_item.get("state") == "unknown":
        return "unknown", "Next Product work has unknown execution state."
    return "unknown", "Product portfolio state could not be derived from current Hermes evidence."


def _portfolio_vision_alignment(backlog: Dict[str, Any]) -> Dict[str, Any]:
    items = backlog.get("items") if isinstance(backlog.get("items"), list) else []
    counts = {
        "total": len(items),
        "linked": 0,
        "unlinked": 0,
        "planned": 0,
        "in_flight": 0,
        "needs_attention": 0,
        "blocked": 0,
        "failed": 0,
        "complete": 0,
        "unknown": 0,
        "linked_goal_count": 0,
        "linked_risk": 0,
        "linked_advancing": 0,
        "unlinked_risk": 0,
    }
    linked_goal_ids: set[str] = set()
    latest_source_at = _float_or_zero((backlog.get("freshness") or {}).get("latest_source_at"))

    for item in items:
        state = str(item.get("state") or "unknown")
        if state not in {"planned", "in_flight", "needs_attention", "blocked", "failed", "complete", "unknown"}:
            state = "unknown"
        counts[state] += 1
        latest_source_at = max(
            latest_source_at,
            _float_or_zero(item.get("updated_at")),
            _float_or_zero((item.get("freshness") or {}).get("latest_source_at")),
        )

        linked_goal_id = str(item.get("linked_goal_id") or "").strip()
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        source_goal_id = str(source.get("goal_id") or "").strip()
        is_goal_backed = bool(linked_goal_id or source_goal_id or source.get("kind") == "dev_project_goal")
        if linked_goal_id:
            linked_goal_ids.add(linked_goal_id)
        elif source_goal_id:
            linked_goal_ids.add(source_goal_id)

        if is_goal_backed:
            counts["linked"] += 1
            if state in {"blocked", "failed", "needs_attention"}:
                counts["linked_risk"] += 1
            elif state in {"planned", "in_flight", "complete"}:
                counts["linked_advancing"] += 1
        else:
            counts["unlinked"] += 1
            if state in {"blocked", "failed"}:
                counts["unlinked_risk"] += 1

    counts["linked_goal_count"] = len(linked_goal_ids)
    updated_at = max(_float_or_zero(backlog.get("updated_at")), latest_source_at) or None

    if counts["linked_risk"] > 0:
        state = "at_risk"
        reason = "Goal-linked Product work is blocked, failed, or waiting for attention."
        source = "backlog_goal_links"
    elif counts["linked_advancing"] > 0:
        state = "on_track"
        reason = "Goal-linked Product work is planned, in flight, or complete."
        source = "backlog_goal_links"
    elif counts["unlinked_risk"] > 0:
        state = "off_track"
        reason = "Execution-derived Product work is failing or blocked without linked goal evidence."
        source = "execution_backlog"
    else:
        state = "unassessed"
        reason = "No linked Product goal evidence is available for vision alignment."
        source = "insufficient_evidence"

    return {
        "object": "hermes.dev_product_vision_alignment",
        "state": state if state in PRODUCT_VISION_ALIGNMENT_STATES else "unassessed",
        "reason": reason,
        "counts": counts,
        "source": source,
        "updated_at": updated_at,
    }


def _portfolio_freshness(backlog: Dict[str, Any]) -> Dict[str, Any]:
    freshness = dict(backlog.get("freshness") or {})
    source_count = int(freshness.get("source_count") or 0)
    latest_source_at = freshness.get("latest_source_at")
    state = freshness.get("state")
    if not state:
        state = "fresh" if source_count > 0 and latest_source_at else "unknown"
    freshness["state"] = state
    if state == "unknown" and "reason" not in freshness:
        freshness["reason"] = "No current Product execution evidence is available."
    return freshness


def _portfolio_counts(items: list[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "total": len(items),
        "needs_attention": 0,
        "active": 0,
        "planned": 0,
        "complete": 0,
        "unknown": 0,
    }
    for item in items:
        state = str(item.get("attention_state") or "unknown")
        counts[state if state in counts else "unknown"] += 1
    return counts


def _portfolio_sort_key(item: Dict[str, Any]) -> tuple[int, int, int, float, str, str]:
    flow_state = normalize_flow_control_state((item.get("flow_control") or {}).get("state"), default="normal")
    flow_rank = {
        "needs_direction": 0,
        "paused": 1,
        "hold_new_work": 1,
        "unknown": 2,
        "normal": 3,
    }.get(flow_state, 3)
    rank = {
        "needs_attention": 0,
        "active": 1,
        "planned": 2,
        "complete": 3,
        "unknown": 4,
    }.get(str(item.get("attention_state") or "unknown"), 4)
    if flow_state in {"paused", "hold_new_work"}:
        rank = min(rank, 1)
    next_order = 0 if item.get("next_item") else 1
    updated_at = -_float_or_zero(item.get("updated_at"))
    return (
        rank,
        flow_rank,
        next_order,
        updated_at,
        str(item.get("name") or "").lower(),
        str(item.get("product_id") or ""),
    )


def _manual_context_by_source(manual_work_items: Iterable[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    context: Dict[tuple[str, str], Dict[str, Any]] = {}
    for item in manual_work_items:
        compact = {
            "id": item.get("id"),
            "title": item.get("title"),
            "status": item.get("status"),
            "notes": item.get("notes"),
        }
        linked_goal_id = str(item.get("linked_goal_id") or item.get("linkedGoalID") or "").strip()
        if linked_goal_id:
            context[(linked_goal_id, "")] = compact
        linked_plan_id = str(item.get("linked_dev_plan_id") or item.get("linkedDevPlanID") or "").strip()
        linked_task_id = str(item.get("linked_task_id") or item.get("linkedTaskID") or "").strip()
        if linked_plan_id and linked_task_id:
            context[(linked_plan_id, linked_task_id)] = compact
    return context


def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def normalize_lifecycle_state(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    return normalized if normalized in PRODUCT_LIFECYCLE_STATES else "unknown"


def normalize_flow_control_state(value: Any, *, default: str = "unknown") -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if not normalized:
        return default if default in PRODUCT_FLOW_CONTROL_STATES else "unknown"
    return normalized if normalized in PRODUCT_FLOW_CONTROL_STATES else "unknown"


def normalize_autonomy_level(value: Any, *, default: str = DEFAULT_PRODUCT_AUTONOMY_LEVEL) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    fallback = default if default in PRODUCT_AUTONOMY_LEVELS or default == "unknown" else DEFAULT_PRODUCT_AUTONOMY_LEVEL
    if not normalized:
        return fallback
    return normalized if normalized in PRODUCT_AUTONOMY_LEVELS else "unknown"


def default_flow_control(*, scope: str = "product") -> Dict[str, Any]:
    return {
        "object": "hermes.dev_portfolio_flow_control" if scope == "portfolio" else "hermes.dev_product_flow_control",
        "scope": scope,
        "state": "normal",
        "autonomy_level": DEFAULT_PRODUCT_AUTONOMY_LEVEL,
        "updated_at": None,
        "requested_by": None,
        "reason": None,
        "source": "default",
    }


def normalize_flow_control(raw: Optional[Dict[str, Any]], *, scope: str = "product") -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return default_flow_control(scope=scope)
    state = normalize_flow_control_state(raw.get("state"), default="normal")
    autonomy_level = normalize_autonomy_level(raw.get("autonomy_level"), default=DEFAULT_PRODUCT_AUTONOMY_LEVEL)
    if autonomy_level == "unknown":
        autonomy_level = DEFAULT_PRODUCT_AUTONOMY_LEVEL
    reason = str(raw.get("reason") or "").strip()
    if state == "unknown":
        reason = reason or f"{scope.title()} flow-control state is unknown."
    updated_at = (_float_or_zero(raw.get("updated_at")) or None)
    requested_by = str(raw.get("requested_by") or "").strip() or None
    result: Dict[str, Any] = {
        "object": "hermes.dev_portfolio_flow_control" if scope == "portfolio" else "hermes.dev_product_flow_control",
        "scope": scope,
        "state": state,
        "autonomy_level": autonomy_level,
        "updated_at": updated_at,
        "requested_by": requested_by,
        "reason": reason or None,
        "source": raw.get("source") if raw.get("source") else "stored",
    }
    previous_state = normalize_flow_control_state(raw.get("previous_state"), default="unknown")
    if previous_state != "unknown":
        result["previous_state"] = previous_state
    previous_autonomy = normalize_autonomy_level(raw.get("previous_autonomy_level"), default="unknown")
    if previous_autonomy != "unknown":
        result["previous_autonomy_level"] = previous_autonomy
    return result


def product_flow_control(product: Dict[str, Any]) -> Dict[str, Any]:
    payload = product.get("payload") if isinstance(product.get("payload"), dict) else {}
    raw = product.get("flow_control")
    if not isinstance(raw, dict):
        raw = payload.get("flow_control") if isinstance(payload.get("flow_control"), dict) else {}
    if not raw:
        return default_flow_control(scope="product")
    return normalize_flow_control(raw, scope="product")


def build_flow_control_update(
    *,
    state: str,
    autonomy_level: Optional[str] = None,
    reason: Optional[str] = None,
    requested_by: Optional[str] = None,
    previous: Optional[Dict[str, Any]] = None,
    scope: str = "product",
    now: Optional[float] = None,
) -> Dict[str, Any]:
    normalized = normalize_flow_control_state(state, default="unknown")
    if normalized not in PRODUCT_FLOW_CONTROL_MUTATION_STATES:
        raise ValueError(f"Unsupported Product flow-control state: {state}")
    previous_state = normalize_flow_control_state((previous or {}).get("state"), default="normal")
    previous_autonomy_level = normalize_autonomy_level(
        (previous or {}).get("autonomy_level"),
        default=DEFAULT_PRODUCT_AUTONOMY_LEVEL,
    )
    if autonomy_level is None:
        normalized_autonomy_level = previous_autonomy_level
    else:
        normalized_autonomy_level = normalize_autonomy_level(autonomy_level, default="unknown")
        if normalized_autonomy_level == "unknown":
            raise ValueError(f"Unsupported Product autonomy level: {autonomy_level}")
    return {
        "object": "hermes.dev_portfolio_flow_control" if scope == "portfolio" else "hermes.dev_product_flow_control",
        "scope": scope,
        "state": normalized,
        "previous_state": previous_state,
        "autonomy_level": normalized_autonomy_level,
        "previous_autonomy_level": previous_autonomy_level,
        "reason": str(reason or "").strip() or None,
        "requested_by": str(requested_by or "").strip() or "operator",
        "updated_at": float(now or time.time()),
    }


def stable_product_id_for_project(project_id: str) -> str:
    resolved = resolve_project_id(project_id, default=DEFAULT_PROJECT_ID)
    digest = hashlib.blake2s(resolved.encode("utf-8"), digest_size=8).hexdigest()
    slug = re.sub(r"[^a-z0-9]+", "-", resolved.lower()).strip("-")[:32] or "product"
    return f"product-{slug}-{digest}"


def _normalize_product_payload(
    payload: Dict[str, Any],
    *,
    deterministic_id: bool = False,
) -> Dict[str, Any]:
    project_id = resolve_project_id(payload.get("project_id"), default=DEFAULT_PROJECT_ID)
    product_id = str(payload.get("product_id") or "").strip()
    if not product_id:
        product_id = stable_product_id_for_project(project_id) if deterministic_id else f"product-{uuid.uuid4().hex[:12]}"
    name = str(payload.get("name") or "").strip() or _display_name_from_project_id(project_id)
    repository_bindings = payload.get("repository_bindings") or []
    if not isinstance(repository_bindings, list):
        repository_bindings = [repository_bindings]
    archived_at = payload.get("archived_at")
    lifecycle_state = normalize_lifecycle_state(payload.get("lifecycle_state"))
    if archived_at is not None:
        lifecycle_state = "archived"
    primary_repo = str(payload.get("primary_repo") or "").strip() or _primary_repo_from_bindings(repository_bindings)
    product_payload = payload.get("payload") or {}
    if not isinstance(product_payload, dict):
        product_payload = {"value": product_payload}
    if isinstance(payload.get("flow_control"), dict):
        product_payload["flow_control"] = product_flow_control({"payload": {"flow_control": payload["flow_control"]}})
    return {
        "object": "hermes.dev_product",
        "product_id": product_id,
        "project_id": project_id,
        "name": name,
        "lifecycle_state": lifecycle_state,
        "primary_repo": primary_repo or None,
        "repository_bindings": repository_bindings,
        "payload": product_payload,
        "flow_control": product_flow_control({"payload": product_payload}),
        "created_at": payload.get("created_at"),
        "updated_at": payload.get("updated_at"),
        "archived_at": float(archived_at) if archived_at is not None else None,
    }


def _row_values(product: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        product["product_id"],
        product["project_id"],
        product["name"],
        normalize_lifecycle_state(product.get("lifecycle_state")),
        product.get("primary_repo"),
        json.dumps(product.get("repository_bindings") or [], ensure_ascii=False),
        json.dumps(product.get("payload") or {}, ensure_ascii=False),
        float(product["created_at"]),
        float(product["updated_at"]),
        product.get("archived_at"),
    )


def _row_to_payload(row: sqlite3.Row) -> Dict[str, Any]:
    payload = _json_loads(row["payload"], {})
    repository_bindings = _json_loads(row["repository_bindings"], [])
    product = {
        "object": "hermes.dev_product",
        "product_id": row["product_id"],
        "project_id": row["project_id"],
        "name": row["name"],
        "lifecycle_state": normalize_lifecycle_state(row["lifecycle_state"]),
        "primary_repo": row["primary_repo"],
        "repository_bindings": repository_bindings if isinstance(repository_bindings, list) else [],
        "payload": payload if isinstance(payload, dict) else {},
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "archived_at": row["archived_at"],
    }
    product["flow_control"] = product_flow_control(product)
    return product


def _normalize_progression_iteration(iteration: Dict[str, Any]) -> Dict[str, Any]:
    product_id = str(iteration.get("product_id") or "").strip()
    if not product_id:
        raise ValueError("Product progression iteration requires product_id")
    project_id = resolve_project_id(iteration.get("project_id"), default=DEFAULT_PROJECT_ID)
    status = str(iteration.get("status") or "idle").strip().lower()
    if status not in PRODUCT_PROGRESSION_STATUSES:
        status = "idle"
    evaluated_at = float(iteration.get("evaluated_at") or time.time())
    iteration_id = str(iteration.get("iteration_id") or "").strip() or _stable_progression_iteration_id(
        product_id=product_id,
        evaluated_at=evaluated_at,
    )
    selected_action = iteration.get("selected_action") if isinstance(iteration.get("selected_action"), dict) else None
    selected_action_id = str(iteration.get("selected_action_id") or (selected_action or {}).get("id") or "").strip() or None
    selected_action_kind = str(iteration.get("selected_action_kind") or (selected_action or {}).get("kind") or "").strip() or None
    source_refs = iteration.get("source_refs") if isinstance(iteration.get("source_refs"), list) else []
    payload = dict(iteration)
    payload["object"] = "hermes.dev_product_progression_iteration"
    payload["product_id"] = product_id
    payload["project_id"] = project_id
    payload["status"] = status
    payload["selected_action_id"] = selected_action_id
    payload["selected_action_kind"] = selected_action_kind
    payload["source_refs"] = source_refs
    payload["evaluated_at"] = evaluated_at
    payload["created_at"] = float(iteration.get("created_at") or evaluated_at)
    return {
        **payload,
        "iteration_id": iteration_id,
        "reason": str(iteration.get("reason") or "").strip() or None,
        "payload": payload,
    }


def _progression_row_to_payload(row: sqlite3.Row) -> Dict[str, Any]:
    payload = _json_loads(row["payload"], {})
    if not isinstance(payload, dict):
        payload = {}
    source_refs = _json_loads(row["source_refs"], [])
    if not isinstance(source_refs, list):
        source_refs = []
    result = {
        **payload,
        "object": "hermes.dev_product_progression_iteration",
        "iteration_id": row["iteration_id"],
        "product_id": row["product_id"],
        "project_id": row["project_id"],
        "status": row["status"],
        "reason": row["reason"],
        "selected_action_id": row["selected_action_id"],
        "selected_action_kind": row["selected_action_kind"],
        "source_refs": source_refs,
        "evaluated_at": row["evaluated_at"],
        "created_at": row["created_at"],
    }
    return result


def _json_loads(raw: Any, default: Any) -> Any:
    try:
        return json.loads(raw) if raw else default
    except Exception:
        return default


def _primary_repo_from_bindings(bindings: Iterable[Any]) -> Optional[str]:
    for binding in bindings:
        if isinstance(binding, dict):
            path = str(binding.get("path") or binding.get("repo") or "").strip()
            if path:
                return path
        else:
            path = str(binding or "").strip()
            if path:
                return path
    return None


def _display_name_from_project_id(project_id: str) -> str:
    value = str(project_id or "").strip() or DEFAULT_PROJECT_ID
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    words = re.sub(r"[_-]+", " ", words).strip()
    return words or "Product"
