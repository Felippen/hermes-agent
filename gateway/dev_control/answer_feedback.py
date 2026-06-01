"""Durable answer-feedback events for Oryn chat observability."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


ANSWER_FEEDBACK_EVENT_TYPE = "ai.answer_feedback"
ANSWER_FEEDBACK_REASONS = {
    "missing_context",
    "wrong_priority",
    "too_vague",
    "unsupported_claim",
    "stale_context",
    "bad_action",
    "wrong_tool_use",
    "not_following_instructions",
    "other",
}
ANSWER_FEEDBACK_RATINGS = {"up", "down"}
DEFAULT_BATCH_LIMIT = 50
MAX_EXCERPT_CHARS = 700
MAX_COMMENT_CHARS = 500
MAX_CONTEXT_KEYS = 32
MAX_CONTEXT_VALUE_CHARS = 240

SECRET_PATTERNS = [
    re.compile(r"(?i)\b(bearer|token|api[_-]?key|authorization|password|secret)\s*[:=]\s*\S+"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    re.compile(r"(?:file://)?/(?:Users|private|var|tmp|Volumes)/[^\s,;:)]+"),
    re.compile(r"\b[A-Za-z0-9_-]{40,}\b"),
]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_answer_feedback (
    event_id TEXT PRIMARY KEY,
    received_at REAL NOT NULL,
    client_ts REAL,
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    profile TEXT NOT NULL,
    mode TEXT,
    model TEXT,
    rating TEXT NOT NULL,
    reason_tags TEXT NOT NULL,
    comment_redacted TEXT,
    user_prompt_excerpt TEXT,
    answer_excerpt TEXT,
    awareness_packet_id TEXT,
    awareness_object_refs TEXT NOT NULL DEFAULT '[]',
    context TEXT NOT NULL DEFAULT '{}',
    laminar_status TEXT,
    judge_scores TEXT NOT NULL DEFAULT '{}',
    judged_at REAL
);

CREATE INDEX IF NOT EXISTS idx_dev_answer_feedback_received_at
    ON dev_answer_feedback(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_answer_feedback_profile_rating
    ON dev_answer_feedback(profile, rating, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_answer_feedback_trace
    ON dev_answer_feedback(trace_id);
"""


class DevAnswerFeedbackStore:
    """SQLite store preserving individual answer-feedback records."""

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

    def ingest_batch(
        self,
        payload: Any,
        *,
        batch_limit: Optional[int] = None,
        export: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        events = payload.get("events") if isinstance(payload, dict) else payload
        if not isinstance(events, list):
            raise ValueError("Answer feedback ingest expects an events array.")
        limit = max(1, min(int(batch_limit or DEFAULT_BATCH_LIMIT), 200))
        accepted = 0
        rejected: list[Dict[str, Any]] = []
        stored: list[Dict[str, Any]] = []
        for index, raw in enumerate(events[:limit]):
            try:
                event = normalize_answer_feedback(raw)
                saved = self.upsert_event(event)
                if export:
                    result = export(saved)
                    self.update_laminar_status(saved["event_id"], str(result.get("status") or "unknown"))
                    saved = self.get_event(saved["event_id"]) or saved
                stored.append(saved)
                accepted += 1
            except Exception as exc:
                rejected.append({"index": index, "reason": str(exc)})
        overflow = max(len(events) - limit, 0)
        if overflow:
            rejected.append({"index": limit, "reason": f"batch limit exceeded; {overflow} event(s) ignored"})
        return {
            "ok": True,
            "object": "hermes.dev_answer_feedback_ingest",
            "accepted": accepted,
            "rejected": len(rejected),
            "rejections": rejected[:20],
            "events": stored,
        }

    def upsert_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_answer_feedback (
                    event_id, received_at, client_ts, session_id, message_id, trace_id,
                    profile, mode, model, rating, reason_tags, comment_redacted,
                    user_prompt_excerpt, answer_excerpt, awareness_packet_id,
                    awareness_object_refs, context
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    client_ts = excluded.client_ts,
                    session_id = excluded.session_id,
                    message_id = excluded.message_id,
                    trace_id = excluded.trace_id,
                    profile = excluded.profile,
                    mode = excluded.mode,
                    model = excluded.model,
                    rating = excluded.rating,
                    reason_tags = excluded.reason_tags,
                    comment_redacted = excluded.comment_redacted,
                    user_prompt_excerpt = excluded.user_prompt_excerpt,
                    answer_excerpt = excluded.answer_excerpt,
                    awareness_packet_id = excluded.awareness_packet_id,
                    awareness_object_refs = excluded.awareness_object_refs,
                    context = excluded.context
                """,
                (
                    event["event_id"],
                    now,
                    event.get("client_ts"),
                    event["session_id"],
                    event["message_id"],
                    event["trace_id"],
                    event["profile"],
                    event.get("mode"),
                    event.get("model"),
                    event["rating"],
                    _json(event.get("reason_tags") or []),
                    event.get("comment_redacted"),
                    event.get("user_prompt_excerpt"),
                    event.get("answer_excerpt"),
                    event.get("awareness_packet_id"),
                    _json(event.get("awareness_object_refs") or []),
                    _json(event.get("context") or {}),
                ),
            )
        return self.get_event(event["event_id"]) or event

    def update_laminar_status(self, event_id: str, status: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE dev_answer_feedback SET laminar_status = ? WHERE event_id = ?",
                (status[:80], event_id),
            )

    def judge_event(
        self,
        event_id: str,
        *,
        scorer: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        export: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        event = self.get_event(event_id)
        if not event:
            raise ValueError(f"answer feedback event not found: {event_id}")
        scores = scorer(event) if scorer else heuristic_judge_scores(event)
        normalized = normalize_judge_scores(scores)
        judged_at = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE dev_answer_feedback SET judge_scores = ?, judged_at = ? WHERE event_id = ?",
                (_json(normalized), judged_at, event_id),
            )
        judged = self.get_event(event_id) or event
        export_status = export(judged) if export else {"status": "not_exported"}
        return {"ok": True, "event": judged, "scores": normalized, "laminar_export": export_status}

    def export_ovyon_fixture(self, event_id: str, output_dir: Path) -> Dict[str, Any]:
        event = self.get_event(event_id)
        if not event:
            raise ValueError(f"answer feedback event not found: {event_id}")
        if event.get("profile") != "ovyon" or event.get("rating") != "down":
            raise ValueError("only negative Ovyon feedback can be exported as an awareness fixture")
        fixture = {
            "case_id": f"feedback-{event['event_id']}",
            "source": "answer_feedback",
            "trace_id": event["trace_id"],
            "session_id": event["session_id"],
            "message_id": event["message_id"],
            "rating": event["rating"],
            "reason_tags": event.get("reason_tags") or [],
            "comment_redacted": event.get("comment_redacted") or "",
            "answer_excerpt": event.get("answer_excerpt") or "",
            "user_prompt_excerpt": event.get("user_prompt_excerpt") or "",
            "awareness_packet_id": event.get("awareness_packet_id") or "",
            "awareness_object_refs": event.get("awareness_object_refs") or [],
            "context": event.get("context") or {},
            "expected_failure_taxonomy": event.get("reason_tags") or [],
        }
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"{fixture['case_id']}.json"
        path.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {"ok": True, "path": str(path), "fixture": fixture}

    def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM dev_answer_feedback WHERE event_id = ?",
            (str(event_id or "").strip(),),
        ).fetchone()
        return _event_from_row(row) if row else None

    def list_events(
        self,
        *,
        profile: Optional[str] = None,
        rating: Optional[str] = None,
        reason: Optional[str] = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if profile:
            clauses.append("profile = ?")
            params.append(str(profile).strip().lower())
        if rating:
            clauses.append("rating = ?")
            params.append(str(rating).strip().lower())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 100), 500)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_answer_feedback
            {where}
            ORDER BY received_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        events = [_event_from_row(row) for row in rows]
        if reason:
            reason = str(reason).strip().lower()
            events = [event for event in events if reason in (event.get("reason_tags") or [])]
        return events


def normalize_answer_feedback(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("event must be an object")
    event_id = _bounded(raw.get("event_id") or raw.get("id"), 160)
    session_id = _bounded(raw.get("session_id"), 160)
    message_id = _bounded(raw.get("message_id"), 160)
    if not event_id:
        raise ValueError("event_id is required")
    if not session_id:
        raise ValueError("session_id is required")
    if not message_id:
        raise ValueError("message_id is required")
    rating = str(raw.get("rating") or "").strip().lower()
    if rating not in ANSWER_FEEDBACK_RATINGS:
        raise ValueError(f"unsupported answer feedback rating: {rating or '<empty>'}")
    reason_tags = _normalized_reasons(raw.get("reason_tags") or raw.get("reasons") or [])
    if rating == "down" and not reason_tags:
        reason_tags = ["other"]
    trace_id = _bounded(raw.get("trace_id"), 80) or answer_feedback_trace_id(session_id, message_id)
    context = _normalized_context(raw.get("context") or {})
    return {
        "event_id": event_id,
        "client_ts": _float_or_none(raw.get("client_ts") or raw.get("timestamp")),
        "session_id": session_id,
        "message_id": message_id,
        "trace_id": trace_id,
        "profile": _bounded(raw.get("profile"), 80).lower() or "unknown",
        "mode": _bounded(raw.get("mode"), 80),
        "model": _bounded(raw.get("model"), 160),
        "rating": rating,
        "reason_tags": reason_tags,
        "comment_redacted": redact_answer_feedback_text(raw.get("comment_redacted") or raw.get("comment"), MAX_COMMENT_CHARS),
        "user_prompt_excerpt": redact_answer_feedback_text(raw.get("user_prompt_excerpt") or raw.get("prompt_excerpt"), MAX_EXCERPT_CHARS),
        "answer_excerpt": redact_answer_feedback_text(raw.get("answer_excerpt"), MAX_EXCERPT_CHARS),
        "awareness_packet_id": _bounded(raw.get("awareness_packet_id"), 160),
        "awareness_object_refs": _normalized_list(raw.get("awareness_object_refs") or raw.get("object_refs") or [], 40, 160),
        "context": context,
    }


def answer_feedback_trace_id(session_id: str, message_id: str) -> str:
    return hashlib.sha256(f"{session_id}:{message_id}".encode("utf-8")).hexdigest()[:32]


def heuristic_judge_scores(event: Dict[str, Any]) -> Dict[str, Any]:
    rating = event.get("rating")
    reasons = set(event.get("reason_tags") or [])
    if rating == "up":
        base = 0.9
    else:
        base = 0.35
    return {
        "judge_usefulness": 0.2 if "too_vague" in reasons or "missing_context" in reasons else base,
        "judge_groundedness": 0.2 if "unsupported_claim" in reasons else base,
        "judge_actionability": 0.2 if "bad_action" in reasons or "wrong_priority" in reasons else base,
        "judge_context_fit": 0.2 if "missing_context" in reasons or "stale_context" in reasons else base,
    }


def normalize_judge_scores(scores: Dict[str, Any]) -> Dict[str, float]:
    allowed = ("judge_usefulness", "judge_groundedness", "judge_actionability", "judge_context_fit")
    normalized: Dict[str, float] = {}
    for key in allowed:
        try:
            value = float(scores.get(key))
        except Exception:
            value = 0.0
        normalized[key] = max(0.0, min(value, 1.0))
    return normalized


def redact_answer_feedback_text(value: Any, max_chars: int) -> str:
    text = str(value or "").strip()
    for pattern in SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _normalized_reasons(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    reasons: list[str] = []
    for item in value:
        reason = str(item or "").strip().lower()
        if reason in ANSWER_FEEDBACK_REASONS and reason not in reasons:
            reasons.append(reason)
    return reasons[:8]


def _normalized_context(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    context: Dict[str, str] = {}
    for key, raw in list(value.items())[:MAX_CONTEXT_KEYS]:
        key_text = str(key or "").strip().lower()
        if not key_text or _drops_context_key(key_text):
            continue
        context[key_text[:64]] = redact_answer_feedback_text(raw, MAX_CONTEXT_VALUE_CHARS)
    return context


def _drops_context_key(key: str) -> bool:
    return any(
        part in key
        for part in (
            "prompt",
            "body",
            "content",
            "user_text",
            "input_text",
            "authorization",
            "api_key",
            "token",
            "secret",
        )
    )


def _normalized_list(value: Any, limit: int, max_chars: int) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value[:limit]:
        text = _bounded(item, max_chars)
        if text:
            items.append(text)
    return items


def _bounded(value: Any, max_chars: int) -> str:
    return str(value or "").strip()[:max_chars]


def _float_or_none(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json_loads(value: Any, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return default


def _event_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "received_at": row["received_at"],
        "client_ts": row["client_ts"],
        "session_id": row["session_id"],
        "message_id": row["message_id"],
        "trace_id": row["trace_id"],
        "profile": row["profile"],
        "mode": row["mode"],
        "model": row["model"],
        "rating": row["rating"],
        "reason_tags": _json_loads(row["reason_tags"], []),
        "comment_redacted": row["comment_redacted"],
        "user_prompt_excerpt": row["user_prompt_excerpt"],
        "answer_excerpt": row["answer_excerpt"],
        "awareness_packet_id": row["awareness_packet_id"],
        "awareness_object_refs": _json_loads(row["awareness_object_refs"], []),
        "context": _json_loads(row["context"], {}),
        "laminar_status": row["laminar_status"],
        "judge_scores": _json_loads(row["judge_scores"], {}),
        "judged_at": row["judged_at"],
    }
