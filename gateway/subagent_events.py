"""Persistent subagent event history backed by Hermes state.db."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS subagent_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at REAL NOT NULL,
    session_id TEXT,
    run_id TEXT,
    subagent_id TEXT NOT NULL,
    parent_id TEXT,
    runtime TEXT,
    ao_session_id TEXT,
    event_type TEXT NOT NULL,
    status TEXT,
    goal TEXT,
    summary TEXT,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_subagent_events_session
    ON subagent_events(session_id, event_id);
CREATE INDEX IF NOT EXISTS idx_subagent_events_run
    ON subagent_events(run_id, event_id);
CREATE INDEX IF NOT EXISTS idx_subagent_events_ao_session
    ON subagent_events(ao_session_id, event_id);

CREATE TABLE IF NOT EXISTS ao_session_prompts (
    ao_session_id TEXT PRIMARY KEY,
    project_id TEXT,
    prompt TEXT NOT NULL,
    goal TEXT,
    issue_id TEXT,
    branch TEXT,
    agent TEXT,
    model TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
"""


class SubagentEventStore:
    """Small append-only event store for normalized ``subagent.*`` payloads."""

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

    def append_event(self, payload: Dict[str, Any], *, session_id: Optional[str] = None) -> Dict[str, Any]:
        event = dict(payload)
        created_at = float(event.get("created_at") or event.get("timestamp") or time.time())
        if session_id:
            event.setdefault("session_id", session_id)
        event["created_at"] = created_at

        with self._lock, self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO subagent_events (
                    created_at, session_id, run_id, subagent_id, parent_id,
                    runtime, ao_session_id, event_type, status, goal, summary, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    event.get("session_id"),
                    event.get("run_id"),
                    event.get("subagent_id"),
                    event.get("parent_id"),
                    event.get("runtime"),
                    event.get("ao_session_id"),
                    event.get("event") or "subagent.progress",
                    event.get("status"),
                    event.get("goal"),
                    event.get("summary"),
                    json.dumps(event, ensure_ascii=False),
                ),
            )
            event_id = int(cur.lastrowid)

        event["event_id"] = event_id
        return event

    def list_events(
        self,
        *,
        session_id: Optional[str] = None,
        run_id: Optional[str] = None,
        ao_session_id: Optional[str] = None,
        limit: int = 500,
    ) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if session_id:
            clauses.append("session_id = ?")
            params.append(session_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if ao_session_id:
            clauses.append("ao_session_id = ?")
            params.append(ao_session_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 500), 2000)))
        rows = self._conn.execute(
            f"""
            SELECT event_id, created_at, payload
            FROM subagent_events
            {where}
            ORDER BY event_id ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def upsert_ao_prompt(
        self,
        *,
        ao_session_id: str,
        project_id: Optional[str],
        prompt: str,
        goal: Optional[str],
        issue_id: Optional[str],
        branch: Optional[str],
        agent: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO ao_session_prompts (
                    ao_session_id, project_id, prompt, goal, issue_id,
                    branch, agent, model, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ao_session_id) DO UPDATE SET
                    project_id = excluded.project_id,
                    prompt = excluded.prompt,
                    goal = excluded.goal,
                    issue_id = excluded.issue_id,
                    branch = excluded.branch,
                    agent = excluded.agent,
                    model = excluded.model,
                    updated_at = excluded.updated_at
                """,
                (ao_session_id, project_id, prompt, goal, issue_id, branch, agent, model, now, now),
            )

    def get_ao_prompt(self, ao_session_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT ao_session_id, project_id, prompt, goal, issue_id, branch, agent, model,
                   created_at, updated_at
            FROM ao_session_prompts
            WHERE ao_session_id = ?
            """,
            (ao_session_id,),
        ).fetchone()
        return dict(row) if row else None

    def latest_event_for_ao_session(self, ao_session_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT event_id, created_at, payload
            FROM subagent_events
            WHERE ao_session_id = ?
            ORDER BY event_id DESC
            LIMIT 1
            """,
            (ao_session_id,),
        ).fetchone()
        return self._row_to_event(row) if row else None

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Dict[str, Any]:
        payload = json.loads(row["payload"])
        payload["event_id"] = int(row["event_id"])
        payload.setdefault("created_at", float(row["created_at"]))
        return payload


def events_response(events: Iterable[Dict[str, Any]], **extra: Any) -> Dict[str, Any]:
    data = list(events)
    return {
        "object": "list",
        "data": data,
        "total": len(data),
        **extra,
    }
