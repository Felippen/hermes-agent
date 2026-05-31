"""Durable Dev clarification sessions for planning-mode vision refinement."""

from __future__ import annotations

import copy
import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from agent.auxiliary_client import call_llm
from gateway.dev_control.acceptance_criteria import (
    ACCEPTANCE_CRITERION_JSON_SCHEMA,
    ALLOWED_VERIFICATION_COMMAND_SHAPES,
    normalize_acceptance_criteria,
    validate_and_downgrade_criteria,
)
from gateway.dev_control.project_scope import DEFAULT_PROJECT_ID, resolve_project_id
from gateway.dev_control.repo_grounding import collect_repo_grounding
from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_clarification_sessions (
    clarification_id TEXT PRIMARY KEY,
    project_id TEXT,
    session_id TEXT,
    status TEXT NOT NULL,
    vision_brief TEXT NOT NULL,
    clarification_kind TEXT NOT NULL DEFAULT 'planning',
    current_question_index INTEGER NOT NULL DEFAULT 0,
    questions TEXT NOT NULL,
    answers TEXT NOT NULL,
    clarified_brief TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL,
    payload TEXT
);

CREATE INDEX IF NOT EXISTS idx_dev_clarification_sessions_updated_at
    ON dev_clarification_sessions(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_clarification_sessions_project_status
    ON dev_clarification_sessions(project_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_clarification_sessions_session
    ON dev_clarification_sessions(session_id, updated_at DESC);
"""

SCHEMA_INDEX_KIND_SQL = """
CREATE INDEX IF NOT EXISTS idx_dev_clarification_sessions_kind
    ON dev_clarification_sessions(clarification_kind, updated_at DESC);
"""

DEFAULT_MAX_QUESTIONS = 5
MIN_TARGET_QUESTIONS = 3
MAX_QUESTION_LIMIT = 5
CLARIFICATION_STATUSES = {"active", "completed", "cancelled", "expired", "brief_ready"}
CLARIFICATION_KINDS = {"planning", "project_onboarding", "feature_onboarding", "project_discovery"}
DEFAULT_CLARIFICATION_KIND = "planning"
PROJECT_ONBOARDING_VISION_SEED = "Project setup"
PROJECT_DISCOVERY_VISION_SEED = "Project discovery"
FEATURE_ONBOARDING_VISION_SEED = "Feature planning"
DISCOVERY_MIN_ANSWERS = 4
DISCOVERY_MAX_QUESTIONS = 12
DISCOVERY_KICKOFF_QUESTION_ID = "disc_kickoff"
DISCOVERY_MIN_NARRATIVE_CHARS = 12
FEATURE_SCOPE_BY_OPTION = {
    "a": "narrow_pilot",
    "b": "complete_workflow",
    "c": "system_foundation",
}
FEATURE_ACCEPTANCE_BY_OPTION = {
    "a": "manual_proof",
    "b": "automated_tests",
    "c": "operational_evidence",
}
ONBOARDING_INTENT_BY_OPTION = {
    "a": "greenfield",
    "b": "existing_codebase",
    "c": "docs_only",
    "d": "ops",
}

QUESTION_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["questions"],
    "properties": {
        "questions": {
            "type": "array",
            "minItems": MIN_TARGET_QUESTIONS,
            "maxItems": MAX_QUESTION_LIMIT,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "question_id",
                    "prompt",
                    "recommended_option_id",
                    "allow_freeform",
                    "reason",
                    "options",
                ],
                "properties": {
                    "question_id": {"type": "string"},
                    "prompt": {"type": "string"},
                    "recommended_option_id": {"type": "string"},
                    "allow_freeform": {"type": "boolean"},
                    "reason": {"type": "string"},
                    "options": {
                        "type": "array",
                        "minItems": 2,
                        "maxItems": 4,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["option_id", "label", "description"],
                            "properties": {
                                "option_id": {"type": "string"},
                                "label": {"type": "string"},
                                "description": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
    },
}

CLARIFIED_BRIEF_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "refined_vision",
        "goals",
        "non_goals",
        "constraints",
        "assumptions",
        "acceptance_criteria",
        "risk_notes",
        "open_questions",
        "suggested_next_action",
    ],
    "properties": {
        "refined_vision": {"type": "string"},
        "goals": {"type": "array", "items": {"type": "string"}},
        "non_goals": {"type": "array", "items": {"type": "string"}},
        "constraints": {"type": "array", "items": {"type": "string"}},
        "assumptions": {"type": "array", "items": {"type": "string"}},
        "acceptance_criteria": {
            "type": "array",
            "minItems": 1,
            "items": ACCEPTANCE_CRITERION_JSON_SCHEMA,
        },
        "risk_notes": {"type": "array", "items": {"type": "string"}},
        "open_questions": {"type": "array", "items": {"type": "string"}},
        "suggested_next_action": {"type": "string"},
    },
}

DISCOVERY_QUESTION_ITEM_SCHEMA: Dict[str, Any] = QUESTION_JSON_SCHEMA["properties"]["questions"]["items"]

DISCOVERY_FOLLOWUP_QUESTION_ITEM_SCHEMA: Dict[str, Any] = copy.deepcopy(DISCOVERY_QUESTION_ITEM_SCHEMA)
DISCOVERY_FOLLOWUP_QUESTION_ITEM_SCHEMA["required"] = [
    field
    for field in DISCOVERY_FOLLOWUP_QUESTION_ITEM_SCHEMA["required"]
    if field != "question_id"
]

DISCOVERY_ADVANCE_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["action"],
    "properties": {
        "action": {"type": "string", "enum": ["continue", "ready"]},
        "reason": {"type": "string"},
        "question": DISCOVERY_FOLLOWUP_QUESTION_ITEM_SCHEMA,
    },
}

DISCOVERY_BRIEF_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "discovery_brief_version",
        "project_name",
        "problem",
        "problem_evidence",
        "vision",
        "success_criteria",
        "users_operators",
        "scope_in",
        "scope_out",
        "parking_lot",
        "assumptions",
        "risks",
        "open_questions",
        "first_bet",
        "repositories",
        "constraints",
        "non_goals",
        "intent_class",
        "suggested_next_action",
    ],
    "properties": {
        "discovery_brief_version": {"type": "integer"},
        "project_name": {"type": "string"},
        "problem": {"type": "string"},
        "problem_evidence": {"type": "array", "items": {"type": "string"}},
        "vision": {"type": "string"},
        "success_criteria": {"type": "array", "items": {"type": "string"}},
        "users_operators": {"type": "array", "items": {"type": "string"}},
        "scope_in": {"type": "array", "items": {"type": "string"}},
        "scope_out": {"type": "array", "items": {"type": "string"}},
        "parking_lot": {"type": "array", "items": {"type": "string"}},
        "assumptions": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
        "open_questions": {"type": "array", "items": {"type": "string"}},
        "first_bet": {"type": "string"},
        "repositories": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["path"],
                "properties": {
                    "label": {"type": "string"},
                    "path": {"type": "string"},
                },
            },
        },
        "constraints": {"type": "array", "items": {"type": "string"}},
        "non_goals": {"type": "array", "items": {"type": "string"}},
        "intent_class": {"type": "string"},
        "suggested_next_action": {"type": "string"},
    },
}


@dataclass
class DevClarificationStore:
    """Persistence for durable Dev clarification sessions."""

    db_path: Optional[Path] = None

    def __post_init__(self) -> None:
        self.db_path = self.db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        apply_wal_with_fallback(self._conn, db_label="state.db")
        with self._conn:
            self._conn.executescript(SCHEMA_SQL)
            self._migrate_schema()

    def _migrate_schema(self) -> None:
        columns = {
            str(row[1])
            for row in self._conn.execute("PRAGMA table_info(dev_clarification_sessions)")
        }
        if not columns:
            return
        if "clarification_kind" not in columns:
            self._conn.execute(
                """
                ALTER TABLE dev_clarification_sessions
                ADD COLUMN clarification_kind TEXT NOT NULL DEFAULT 'planning'
                """
            )
        self._conn.executescript(SCHEMA_INDEX_KIND_SQL)

    def close(self) -> None:
        self._conn.close()

    def create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = float(payload.get("created_at") or time.time())
        payload = dict(payload)
        payload["created_at"] = now
        payload["updated_at"] = now
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO dev_clarification_sessions (
                    clarification_id, project_id, session_id, status, vision_brief,
                    clarification_kind, current_question_index, questions, answers, clarified_brief,
                    created_at, updated_at, completed_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _row_values(payload),
            )
        return self.get(payload["clarification_id"]) or payload

    def update(self, clarification_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get(clarification_id)
        if not current:
            raise KeyError(f"Clarification session not found: {clarification_id}")
        payload = {**current, **updates, "updated_at": time.time()}
        with self._conn:
            self._conn.execute(
                """
                UPDATE dev_clarification_sessions
                SET project_id = ?, session_id = ?, status = ?, vision_brief = ?,
                    clarification_kind = ?, current_question_index = ?, questions = ?, answers = ?,
                    clarified_brief = ?, created_at = ?, updated_at = ?,
                    completed_at = ?, payload = ?
                WHERE clarification_id = ?
                """,
                (*_row_values(payload)[1:], clarification_id),
            )
        return self.get(clarification_id) or payload

    def get(self, clarification_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM dev_clarification_sessions
            WHERE clarification_id = ?
            """,
            (str(clarification_id or "").strip(),),
        ).fetchone()
        return _row_to_payload(row) if row else None

    def list(
        self,
        *,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        clarification_kind: Optional[str] = None,
        limit: int = 50,
    ) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if project_id:
            clauses.append("project_id = ?")
            params.append(str(project_id).strip())
        if session_id:
            clauses.append("session_id = ?")
            params.append(str(session_id).strip())
        if status:
            clauses.append("status = ?")
            params.append(str(status).strip())
        if clarification_kind:
            clauses.append("clarification_kind = ?")
            params.append(str(clarification_kind).strip())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM dev_clarification_sessions
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_row_to_payload(row) for row in rows]


def start_clarification(
    *,
    store: DevClarificationStore,
    vision_brief: str,
    project_id: Optional[str] = None,
    session_id: Optional[str] = None,
    project_context: Optional[Dict[str, Any]] = None,
    max_questions: int = DEFAULT_MAX_QUESTIONS,
    clarification_kind: str = DEFAULT_CLARIFICATION_KIND,
) -> Dict[str, Any]:
    kind = _normalize_clarification_kind(clarification_kind)
    normalized_project_context = _normalize_project_context(project_context, project_id=project_id)
    brief = str(vision_brief or "").strip()
    if not brief:
        if kind == "project_onboarding":
            brief = PROJECT_ONBOARDING_VISION_SEED
        elif kind == "project_discovery":
            brief = PROJECT_DISCOVERY_VISION_SEED
        elif kind == "feature_onboarding":
            brief = str((normalized_project_context or {}).get("vision") or "").strip() or FEATURE_ONBOARDING_VISION_SEED
        else:
            raise ValueError("vision_brief is required")
    question_count = max(MIN_TARGET_QUESTIONS, min(int(max_questions or DEFAULT_MAX_QUESTIONS), MAX_QUESTION_LIMIT))
    generation_mode = "llm"
    warning = None
    initial_narrative = None
    resolved_project_id = resolve_project_id(
        project_id,
        (normalized_project_context or {}).get("project_id"),
    )
    grounding_result = collect_repo_grounding(
        repositories=(normalized_project_context or {}).get("repositories") or [],
        vision_brief=brief,
    )
    grounding = grounding_result.get("grounding") or {}
    grounding_provenance = grounding_result.get("provenance") or []
    grounding_warnings = grounding_result.get("warnings") or []
    if kind == "project_onboarding":
        questions = _project_onboarding_questions(
            project_context=normalized_project_context,
        )
        generation_mode = "deterministic"
    elif kind == "feature_onboarding":
        questions = _feature_onboarding_questions(
            project_context=normalized_project_context,
            vision_brief=brief,
        )
        generation_mode = "deterministic"
    elif kind == "project_discovery":
        generation_mode = "adaptive"
        if _is_placeholder_discovery_brief(brief):
            questions = _discovery_kickoff_questions(
                project_context=normalized_project_context,
            )
            initial_narrative = None
        else:
            questions = []
            initial_narrative = brief
    else:
        try:
            questions = _generate_questions(
                brief,
                max_questions=question_count,
                project_context=normalized_project_context,
                grounding=grounding,
            )
        except Exception as exc:
            generation_mode = "fallback"
            warning = f"LLM question generation failed; using deterministic fallback questions: {exc}"
            questions = _fallback_questions(max_questions=question_count)
    payload = {
        "object": "hermes.dev_clarification",
        "clarification_id": f"devclar-{uuid.uuid4().hex[:10]}",
        "project_id": resolved_project_id,
        "session_id": session_id,
        "status": "active",
        "vision_brief": brief,
        "clarification_kind": kind,
        "project_context": normalized_project_context,
        "current_question_index": 0,
        "questions": questions,
        "answers": [],
        "clarified_brief": None,
        "grounding": grounding,
        "grounding_provenance": grounding_provenance,
        "grounding_warnings": grounding_warnings,
        "completed_at": None,
        "generation_mode": generation_mode,
        "warning": warning,
        "discovery_turn": 0 if kind == "project_discovery" else None,
        "discovery_ready": False if kind == "project_discovery" else None,
        "discovery_ready_reason": None if kind == "project_discovery" else None,
        "initial_narrative": initial_narrative if kind == "project_discovery" else None,
    }
    created = store.create(payload)
    if kind == "project_discovery" and initial_narrative:
        try:
            created = _bootstrap_discovery_from_narrative(
                store=store,
                clarification_id=created["clarification_id"],
                payload=created,
            )
        except Exception as exc:
            generation_mode = "fallback"
            warning = _combine_warning(warning, f"First discovery question failed; using fallback: {exc}")
            created = _bootstrap_discovery_from_narrative(
                store=store,
                clarification_id=created["clarification_id"],
                payload=created,
                force_fallback=True,
            )
            created = store.update(created["clarification_id"], {
                "generation_mode": generation_mode,
                "warning": warning,
            })
    return _with_current_question(created)


def list_clarifications(
    *,
    store: DevClarificationStore,
    project_id: Optional[str] = None,
    session_id: Optional[str] = None,
    status: Optional[str] = None,
    clarification_kind: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    normalized_kind = None
    if clarification_kind:
        normalized_kind = _normalize_clarification_kind(clarification_kind)
    data = [_with_current_question(item) for item in store.list(
        project_id=project_id,
        session_id=session_id,
        status=status,
        clarification_kind=normalized_kind,
        limit=limit,
    )]
    return {"object": "list", "data": data, "total": len(data)}


def get_clarification(*, store: DevClarificationStore, clarification_id: str) -> Dict[str, Any]:
    payload = store.get(clarification_id)
    if not payload:
        raise KeyError(f"Clarification session not found: {clarification_id}")
    return _with_current_question(payload)


def answer_clarification(
    *,
    store: DevClarificationStore,
    clarification_id: str,
    question_id: Optional[str] = None,
    option_id: Optional[str] = None,
    answer_text: Optional[str] = None,
    skipped: bool = False,
    back: bool = False,
) -> Dict[str, Any]:
    payload = get_clarification(store=store, clarification_id=clarification_id)
    if payload["status"] != "active":
        raise ValueError(f"Clarification session is {payload['status']}, not active")
    questions = payload.get("questions") or []
    current_index = int(payload.get("current_question_index") or 0)
    if back:
        return _with_current_question(store.update(clarification_id, {
            "current_question_index": max(0, current_index - 1),
        }))
    if not questions:
        raise ValueError("Clarification session has no questions")
    if current_index >= len(questions):
        current_index = len(questions) - 1
    question = questions[current_index]
    expected_question_id = question.get("question_id")
    if question_id and question_id != expected_question_id:
        raise ValueError(f"Answer targets {question_id}, but current question is {expected_question_id}")

    option = _find_option(question, option_id)
    answer = {
        "question_id": expected_question_id,
        "question_prompt": question.get("prompt"),
        "option_id": option.get("option_id") if option else option_id,
        "option_label": option.get("label") if option else None,
        "answer_text": str(answer_text or "").strip() or None,
        "skipped": bool(skipped),
        "answered_at": time.time(),
    }
    answers = [item for item in payload.get("answers") or [] if item.get("question_id") != expected_question_id]
    answers.append(answer)
    next_index = min(current_index + 1, len(questions))
    updated = store.update(clarification_id, {
        "answers": answers,
        "current_question_index": next_index,
    })
    if updated.get("clarification_kind") == "project_discovery":
        narrative_updates: Dict[str, Any] = {}
        if expected_question_id == DISCOVERY_KICKOFF_QUESTION_ID:
            narrative = str(answer_text or "").strip()
            if not narrative and option:
                narrative = str(option.get("label") or "").strip()
            if narrative:
                narrative_updates["vision_brief"] = narrative
                narrative_updates["initial_narrative"] = narrative
        if narrative_updates:
            updated = store.update(clarification_id, narrative_updates)
        questions_after = updated.get("questions") or []
        if next_index >= len(questions_after):
            updated = _advance_discovery_session(store=store, clarification_id=clarification_id, payload=updated)
    return _with_current_question(updated)


def complete_clarification(*, store: DevClarificationStore, clarification_id: str) -> Dict[str, Any]:
    payload = get_clarification(store=store, clarification_id=clarification_id)
    if payload.get("clarification_kind") == "project_discovery":
        if payload["status"] != "active":
            raise ValueError(f"Clarification session is {payload['status']} and cannot be completed")
        if not payload.get("can_complete"):
            raise ValueError("Discovery session is not ready to synthesize a brief yet")
        clarified, profile_warning = _build_discovery_brief(payload)
        warning = _combine_warning(payload.get("warning"), profile_warning)
        return _with_current_question(store.update(clarification_id, {
            "status": "brief_ready",
            "current_question_index": len(payload.get("questions") or []),
            "clarified_brief": clarified,
            "warning": warning,
        }))
    if payload["status"] not in {"active", "completed"}:
        raise ValueError(f"Clarification session is {payload['status']} and cannot be completed")
    if payload.get("clarification_kind") == "project_onboarding":
        clarified, profile_warning = _build_project_onboarding_profile(payload)
        warning = _combine_warning(payload.get("warning"), profile_warning)
    elif payload.get("clarification_kind") == "feature_onboarding":
        clarified, profile_warning = _build_feature_onboarding_brief(payload)
        warning = _combine_warning(payload.get("warning"), profile_warning)
    else:
        clarified = _build_clarified_brief(payload)
        warning = _combine_warning(payload.get("warning"), clarified.get("warning"))
        clarified = {key: value for key, value in clarified.items() if key != "warning"}
    return _with_current_question(store.update(clarification_id, {
        "status": "completed",
        "current_question_index": len(payload.get("questions") or []),
        "clarified_brief": clarified,
        "warning": warning,
        "completed_at": time.time(),
    }))


def cancel_clarification(
    *,
    store: DevClarificationStore,
    clarification_id: str,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    payload = get_clarification(store=store, clarification_id=clarification_id)
    if payload["status"] == "completed":
        raise ValueError("Completed clarification sessions cannot be cancelled")
    updated = store.update(clarification_id, {
        "status": "cancelled",
        "completed_at": time.time(),
        "warning": str(reason or "").strip() or None,
    })
    return _with_current_question(updated)


def approve_clarification_brief(*, store: DevClarificationStore, clarification_id: str) -> Dict[str, Any]:
    payload = get_clarification(store=store, clarification_id=clarification_id)
    if payload.get("clarification_kind") != "project_discovery":
        raise ValueError("Brief approval is only supported for project_discovery sessions")
    if payload["status"] != "brief_ready":
        raise ValueError(f"Clarification session is {payload['status']}, not brief_ready")
    if not payload.get("clarified_brief"):
        raise ValueError("Discovery brief is missing")
    now = time.time()
    return _with_current_question(store.update(clarification_id, {
        "status": "completed",
        "completed_at": now,
        "brief_approved_at": now,
    }))


def revise_clarification_brief(
    *,
    store: DevClarificationStore,
    clarification_id: str,
    feedback: str,
) -> Dict[str, Any]:
    payload = get_clarification(store=store, clarification_id=clarification_id)
    if payload.get("clarification_kind") != "project_discovery":
        raise ValueError("Brief revision is only supported for project_discovery sessions")
    if payload["status"] != "brief_ready":
        raise ValueError(f"Clarification session is {payload['status']}, not brief_ready")
    revision_feedback = str(feedback or "").strip()
    if not revision_feedback:
        raise ValueError("feedback is required")
    payload = dict(payload)
    payload["revision_feedback"] = revision_feedback
    clarified, profile_warning = _build_discovery_brief(payload)
    warning = _combine_warning(payload.get("warning"), profile_warning)
    return _with_current_question(store.update(clarification_id, {
        "clarified_brief": clarified,
        "warning": warning,
        "revision_feedback": revision_feedback,
    }))


def _row_values(payload: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["clarification_id"],
        payload.get("project_id"),
        payload.get("session_id"),
        payload["status"],
        payload["vision_brief"],
        _normalize_clarification_kind(payload.get("clarification_kind")),
        int(payload.get("current_question_index") or 0),
        json.dumps(payload.get("questions") or [], ensure_ascii=False),
        json.dumps(payload.get("answers") or [], ensure_ascii=False),
        json.dumps(payload.get("clarified_brief"), ensure_ascii=False) if payload.get("clarified_brief") is not None else None,
        float(payload["created_at"]),
        float(payload["updated_at"]),
        payload.get("completed_at"),
        json.dumps(payload, ensure_ascii=False),
    )


def _row_to_payload(row: sqlite3.Row) -> Dict[str, Any]:
    payload = json.loads(row["payload"] or "{}")
    clarification_kind = str(
        row["clarification_kind"]
        if "clarification_kind" in row.keys() and row["clarification_kind"]
        else payload.get("clarification_kind")
        or DEFAULT_CLARIFICATION_KIND
    ).strip() or DEFAULT_CLARIFICATION_KIND
    payload.update({
        "object": "hermes.dev_clarification",
        "clarification_id": row["clarification_id"],
        "project_id": row["project_id"],
        "session_id": row["session_id"],
        "status": row["status"],
        "vision_brief": row["vision_brief"],
        "clarification_kind": _normalize_clarification_kind(clarification_kind),
        "current_question_index": int(row["current_question_index"] or 0),
        "questions": json.loads(row["questions"] or "[]"),
        "answers": json.loads(row["answers"] or "[]"),
        "clarified_brief": json.loads(row["clarified_brief"]) if row["clarified_brief"] else None,
        "created_at": float(row["created_at"]),
        "updated_at": float(row["updated_at"]),
        "completed_at": row["completed_at"],
    })
    return payload


def _with_current_question(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload)
    questions = payload.get("questions") or []
    index = int(payload.get("current_question_index") or 0)
    status = payload.get("status")
    payload["current_question"] = questions[index] if 0 <= index < len(questions) and status == "active" else None
    answered_question_ids = {
        item.get("question_id")
        for item in (payload.get("answers") or [])
        if item.get("question_id") and not item.get("skipped")
    }
    answered_count = len(answered_question_ids)
    if payload.get("clarification_kind") == "project_discovery":
        at_max = len(questions) >= DISCOVERY_MAX_QUESTIONS
        discovery_ready = bool(payload.get("discovery_ready"))
        past_last = index >= len(questions)
        payload["can_complete"] = (
            status == "active"
            and answered_count >= DISCOVERY_MIN_ANSWERS
            and (discovery_ready or at_max or past_last)
        )
        return payload
    minimum_answers = min(len(questions), MIN_TARGET_QUESTIONS)
    payload["can_complete"] = answered_count >= minimum_answers or index >= minimum_answers
    return payload


def _generate_questions(
    vision_brief: str,
    *,
    max_questions: int,
    project_context: Optional[Dict[str, Any]] = None,
    grounding: Optional[Dict[str, Any]] = None,
) -> list[Dict[str, Any]]:
    messages = [
        {
            "role": "system",
            "content": (
                "You generate specific planning clarification questions for software/product work. "
                "Return only valid JSON. No markdown. No prose outside JSON. "
                "Questions must be tailored to the user's exact vision brief. Avoid generic project-management wording. "
                "Prefer questions that clarify product behavior, workflow boundaries, data/state needs, success criteria, and risks. "
                "The JSON schema is: "
                "{\"questions\":[{\"question_id\":\"q1\",\"prompt\":\"...\","
                "\"recommended_option_id\":\"a\",\"allow_freeform\":true,\"reason\":\"...\","
                "\"options\":[{\"option_id\":\"a\",\"label\":\"...\",\"description\":\"...\"}]}]}. "
                "Generate 3 to 5 questions. Each question must have 2 to 4 concrete options. "
                "Use short option labels and descriptions that are concrete enough for a plan draft."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Vision brief:\n{vision_brief}\n\n"
                f"Project context:\n{json.dumps(project_context or {}, ensure_ascii=False)}\n\n"
                f"Read-only repository grounding:\n{json.dumps(grounding or {}, ensure_ascii=False)}\n\n"
                f"Max questions: {max_questions}"
            ),
        },
    ]
    content = _call_question_llm(messages)
    try:
        return _validate_questions(_extract_json(content), max_questions=max_questions)
    except Exception:
        repair_messages = [
            messages[0],
            {"role": "user", "content": f"Repair this into valid schema JSON only:\n{content}"},
        ]
        repaired = _call_question_llm(repair_messages)
        return _validate_questions(_extract_json(repaired), max_questions=max_questions)


def _call_question_llm(messages: list[Dict[str, str]]) -> str:
    kwargs = {
        "task": "dev_clarification",
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 3000,
        "timeout": 55,
    }
    try:
        response = call_llm(
            **kwargs,
            extra_body={
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "dev_clarification_questions",
                        "schema": QUESTION_JSON_SCHEMA,
                        "strict": True,
                    },
                },
            },
        )
    except Exception as exc:
        error_text = str(exc).lower()
        if "response_format" not in error_text and "json_schema" not in error_text and "unsupported" not in error_text:
            raise
        response = call_llm(**kwargs)
    return str(response.choices[0].message.content or "").strip()


def _extract_json(content: str) -> Dict[str, Any]:
    text = str(content or "").strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start:end + 1]
    return _loads_json_with_small_repairs(text)


def _loads_json_with_small_repairs(text: str) -> Dict[str, Any]:
    cleaned = re.sub(r",\s*([}\]])", r"\1", str(text or "").strip())
    decoder = json.JSONDecoder()
    try:
        parsed, _ = decoder.raw_decode(cleaned)
    except json.JSONDecodeError:
        parsed = json.loads(cleaned)
    if not isinstance(parsed, dict):
        raise ValueError("LLM response JSON root must be an object")
    return parsed


def _fallback_questions(*, max_questions: int) -> list[Dict[str, Any]]:
    questions = [
        {
            "question_id": "q1",
            "prompt": "What should this idea primarily improve first?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "The first planning split should identify the main value path before implementation details.",
            "options": [
                {"option_id": "a", "label": "User workflow", "description": "Make the end-to-end user experience clearer and easier to complete."},
                {"option_id": "b", "label": "System reliability", "description": "Reduce failures, unclear states, and recovery friction."},
                {"option_id": "c", "label": "Developer leverage", "description": "Improve how Dev plans, delegates, verifies, or ships work."},
            ],
        },
        {
            "question_id": "q2",
            "prompt": "How much should the first version do?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "A bounded first version keeps planning useful without prematurely expanding scope.",
            "options": [
                {"option_id": "a", "label": "Narrow pilot", "description": "Build the smallest useful path and prove it with one workflow."},
                {"option_id": "b", "label": "Complete workflow", "description": "Cover the full expected user journey, but avoid advanced automation."},
                {"option_id": "c", "label": "System foundation", "description": "Prioritize durable architecture and APIs before richer UI behavior."},
            ],
        },
        {
            "question_id": "q3",
            "prompt": "What should count as success for this phase?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Acceptance criteria let Dev turn the clarified brief into a concrete implementation plan later.",
            "options": [
                {"option_id": "a", "label": "Manual proof", "description": "Felipe can complete the flow once and confirm the result is useful."},
                {"option_id": "b", "label": "Automated tests", "description": "The core behavior is covered by backend and app tests."},
                {"option_id": "c", "label": "Operational evidence", "description": "The system records enough state and diagnostics to debug failures."},
            ],
        },
        {
            "question_id": "q4",
            "prompt": "What should stay out of scope for now?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Explicit non-goals keep the next plan from turning into a broad platform rewrite.",
            "options": [
                {"option_id": "a", "label": "No automatic execution", "description": "Do not launch workers or mutate policies from this planning flow."},
                {"option_id": "b", "label": "No new UI surface", "description": "Reuse existing composer and sidebar surfaces where possible."},
                {"option_id": "c", "label": "No broad refactor", "description": "Keep changes scoped to the planning/control-plane path."},
            ],
        },
        {
            "question_id": "q5",
            "prompt": "What risk should Dev watch most closely?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Risk selection helps shape verification and rollout strategy.",
            "options": [
                {"option_id": "a", "label": "Ambiguous output", "description": "The system produces a plan that sounds good but is not actionable."},
                {"option_id": "b", "label": "UI dead ends", "description": "The flow loses state, hides results, or gives no clear next action."},
                {"option_id": "c", "label": "Wrong automation", "description": "The system starts implementation before Felipe explicitly approves it."},
            ],
        },
    ]
    return questions[:max(MIN_TARGET_QUESTIONS, min(int(max_questions or DEFAULT_MAX_QUESTIONS), MAX_QUESTION_LIMIT))]


def _normalize_clarification_kind(value: Any) -> str:
    kind = str(value or DEFAULT_CLARIFICATION_KIND).strip().lower()
    if kind not in CLARIFICATION_KINDS:
        raise ValueError(f"Unsupported clarification_kind: {value}")
    return kind


def _project_onboarding_questions(
    *,
    project_context: Optional[Dict[str, Any]] = None,
) -> list[Dict[str, Any]]:
    project_name = str((project_context or {}).get("project_name") or "").strip() or "New Project"
    return [
        {
            "question_id": "onb_name",
            "prompt": "What should we call this project?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "The display name anchors the project dashboard and Hermes project_id context.",
            "options": [
                {"option_id": "a", "label": project_name, "description": f"Keep the current name: {project_name}."},
                {"option_id": "b", "label": "Rename later", "description": "Use a temporary name now and refine it in the profile editor."},
            ],
        },
        {
            "question_id": "onb_intent",
            "prompt": "What kind of project is this?",
            "recommended_option_id": "b",
            "allow_freeform": False,
            "reason": "Intent class decides whether Dev should bind a repository now or defer it.",
            "options": [
                {"option_id": "a", "label": "Greenfield", "description": "Starting fresh with little or no existing codebase yet."},
                {"option_id": "b", "label": "Existing codebase", "description": "Work continues in one or more repos that already exist."},
                {"option_id": "c", "label": "Docs only", "description": "Vision, specs, or ops notes without a primary code repo."},
                {"option_id": "d", "label": "Ops", "description": "Infrastructure, automation, or operational workflows dominate."},
            ],
        },
        {
            "question_id": "onb_vision",
            "prompt": "In one or two sentences, what should this project achieve?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Vision becomes the north star in Oryn and the Hermes vision goal.",
            "options": [
                {"option_id": "a", "label": "Ship a user-facing improvement", "description": "Focus on a concrete product or workflow outcome."},
                {"option_id": "b", "label": "Improve developer leverage", "description": "Make planning, execution, or verification faster for Dev."},
                {"option_id": "c", "label": "Stabilize the platform", "description": "Reduce failures, drift, or operational toil before adding features."},
            ],
        },
        {
            "question_id": "onb_repo",
            "prompt": "Where is the primary codebase on this machine?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Repo grounding helps Dev ask better follow-up questions and route workers.",
            "options": [
                {"option_id": "a", "label": "Provide a path", "description": "Enter the absolute path to the repo root in freeform text."},
                {"option_id": "b", "label": "Link later", "description": "Skip for now and bind repos from the project profile editor."},
            ],
        },
        {
            "question_id": "onb_extra_repos",
            "prompt": "Any additional repository paths for this project?",
            "recommended_option_id": "b",
            "allow_freeform": True,
            "reason": "Multi-repo projects need every codebase bound before feature planning.",
            "options": [
                {"option_id": "a", "label": "Add more paths", "description": "Enter one absolute path per line in freeform text."},
                {"option_id": "b", "label": "Just the primary repo", "description": "Skip additional repositories for now."},
            ],
        },
        {
            "question_id": "onb_constraints",
            "prompt": "What should stay out of scope for now?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Explicit non-goals keep the first planning cycles bounded.",
            "options": [
                {"option_id": "a", "label": "No broad refactor", "description": "Keep changes scoped to the immediate product goal."},
                {"option_id": "b", "label": "No automatic execution", "description": "Planning and approval only until Felipe explicitly launches work."},
                {"option_id": "c", "label": "No new UI surface", "description": "Reuse existing dashboard and composer surfaces where possible."},
            ],
        },
    ]


def _answers_by_question_id(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for answer in payload.get("answers") or []:
        if not isinstance(answer, dict):
            continue
        question_id = str(answer.get("question_id") or "").strip()
        if question_id:
            indexed[question_id] = answer
    return indexed


def _answer_text_value(answer: Optional[Dict[str, Any]]) -> str:
    if not isinstance(answer, dict):
        return ""
    if answer.get("skipped"):
        return ""
    text = str(answer.get("answer_text") or "").strip()
    if text:
        return text
    return str(answer.get("option_label") or "").strip()


def _scope_exclusion_from_answer(answer: Optional[Dict[str, Any]]) -> list[str]:
    """Map out-of-scope answers to non_goals regardless of option vs freeform."""
    if not isinstance(answer, dict) or answer.get("skipped"):
        return []
    text = str(answer.get("answer_text") or "").strip()
    if text:
        return [text]
    label = str(answer.get("option_label") or "").strip()
    return [label] if label else []


def _build_project_onboarding_profile(payload: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[str]]:
    answers = _answers_by_question_id(payload)
    project_context = payload.get("project_context") or {}
    default_name = str(project_context.get("project_name") or "").strip() or "New Project"

    name_answer = answers.get("onb_name")
    project_name = _answer_text_value(name_answer) or default_name

    intent_answer = answers.get("onb_intent") or {}
    intent_option = str(intent_answer.get("option_id") or "b").strip().lower()
    intent_class = ONBOARDING_INTENT_BY_OPTION.get(intent_option, "existing_codebase")

    vision = _answer_text_value(answers.get("onb_vision"))
    if not vision:
        vision = str(payload.get("vision_brief") or PROJECT_ONBOARDING_VISION_SEED).strip()

    repo_answer = answers.get("onb_repo") or {}
    repos_deferred = bool(repo_answer.get("skipped")) or str(repo_answer.get("option_id") or "").strip().lower() == "b"
    repo_path = ""
    if not repos_deferred:
        repo_path = _answer_text_value(repo_answer)
        if not repo_path and str(repo_answer.get("option_id") or "").strip().lower() == "a":
            repos_deferred = True

    constraints_answer = answers.get("onb_constraints") or {}
    constraints: list[str] = []
    non_goals = _scope_exclusion_from_answer(constraints_answer)

    repositories: list[Dict[str, Any]] = []
    warning = None
    if repo_path:
        expanded = Path(repo_path).expanduser()
        if not expanded.exists():
            warning = f"Repository path does not exist: {repo_path}"
        repositories.append({
            "label": project_name,
            "path": str(expanded),
        })

    extra_repo_answer = answers.get("onb_extra_repos") or {}
    if not extra_repo_answer.get("skipped") and str(extra_repo_answer.get("option_id") or "").strip().lower() != "b":
        extra_text = str(extra_repo_answer.get("answer_text") or "").strip()
        for line_number, raw_line in enumerate(extra_text.splitlines(), start=1):
            extra_path = raw_line.strip()
            if not extra_path:
                continue
            expanded_extra = Path(extra_path).expanduser()
            if not expanded_extra.exists():
                path_warning = f"Additional repository path does not exist: {extra_path}"
                warning = path_warning if not warning else f"{warning}; {path_warning}"
            label = expanded_extra.name or f"Repo {line_number}"
            repositories.append({
                "label": label,
                "path": str(expanded_extra),
            })

    profile = {
        "project_name": project_name,
        "intent_class": intent_class,
        "vision": vision,
        "repositories": repositories,
        "constraints": constraints,
        "non_goals": non_goals,
        "repos_deferred": repos_deferred or not repositories,
    }
    return profile, warning


def _feature_onboarding_questions(
    *,
    project_context: Optional[Dict[str, Any]] = None,
    vision_brief: str = "",
) -> list[Dict[str, Any]]:
    repositories = (project_context or {}).get("repositories") or []
    project_name = str((project_context or {}).get("project_name") or "").strip() or "this project"
    vision_hint = str((project_context or {}).get("vision") or vision_brief or "").strip()
    questions = [
        {
            "question_id": "feat_outcome",
            "prompt": "What should exist when this feature is done?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "The outcome anchors the work item, goal, and plan artifact.",
            "options": [
                {"option_id": "a", "label": "User-visible behavior", "description": "Felipe can complete a concrete workflow and see the result."},
                {"option_id": "b", "label": "Developer workflow", "description": "Planning, execution, or verification becomes materially easier."},
                {"option_id": "c", "label": "Platform capability", "description": "A durable API, control-plane, or data path exists for later work."},
            ],
        },
        {
            "question_id": "feat_scope",
            "prompt": "How much should the first slice include?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Scope keeps the first plan bounded and shippable.",
            "options": [
                {"option_id": "a", "label": "Narrow pilot", "description": "Prove one path end to end before expanding."},
                {"option_id": "b", "label": "Complete workflow", "description": "Cover the expected user journey without advanced automation."},
                {"option_id": "c", "label": "System foundation", "description": "Prioritize durable architecture before richer behavior."},
            ],
        },
        {
            "question_id": "feat_acceptance",
            "prompt": "What should count as success for this feature?",
            "recommended_option_id": "a",
            "allow_freeform": False,
            "reason": "Acceptance criteria drive verification and the plan artifact.",
            "options": [
                {"option_id": "a", "label": "Manual proof", "description": "Felipe can complete the flow once and confirm it is useful."},
                {"option_id": "b", "label": "Automated tests", "description": "Core behavior is covered by backend or app tests."},
                {"option_id": "c", "label": "Operational evidence", "description": "Enough diagnostics and state exist to debug failures."},
            ],
        },
    ]
    if len(repositories) > 1:
        repo_labels = ", ".join(
            str((repo or {}).get("label") or (repo or {}).get("path") or "Repository").strip()
            for repo in repositories[:4]
            if isinstance(repo, dict)
        )
        questions.append({
            "question_id": "feat_repo",
            "prompt": f"Which repository should Dev focus on first for {project_name}?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Multi-repo projects need an explicit first surface for planning and workers.",
            "options": [
                {"option_id": "a", "label": "Primary product repo", "description": f"Bound repos: {repo_labels}."},
                {"option_id": "b", "label": "All bound repos", "description": "Plan across every repository in this project."},
            ],
        })
    questions.append({
        "question_id": "feat_constraints",
        "prompt": "What should stay out of scope for this feature?",
        "recommended_option_id": "a",
        "allow_freeform": True,
        "reason": "Explicit non-goals keep the plan from expanding into a platform rewrite.",
        "options": [
            {"option_id": "a", "label": "No broad refactor", "description": "Keep changes scoped to the feature outcome."},
            {"option_id": "b", "label": "No automatic execution", "description": "Planning and approval only until Felipe launches work."},
            {"option_id": "c", "label": "No new UI surface", "description": "Reuse existing dashboard and composer surfaces where possible."},
        ],
    })
    if vision_hint:
        questions[0]["reason"] = f"{questions[0]['reason']} Project vision: {vision_hint[:160]}"
    return questions


def _feature_title_from_outcome(outcome: str, *, fallback: str) -> str:
    text = str(outcome or "").strip() or fallback
    first_line = text.splitlines()[0].strip()
    sentence = first_line.split(".")[0].strip() or first_line
    return sentence[:120] or fallback


def _build_feature_onboarding_brief(payload: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[str]]:
    answers = _answers_by_question_id(payload)
    project_context = payload.get("project_context") or {}
    vision_seed = str(project_context.get("vision") or payload.get("vision_brief") or "").strip()

    outcome = _answer_text_value(answers.get("feat_outcome")) or vision_seed or FEATURE_ONBOARDING_VISION_SEED
    scope_answer = answers.get("feat_scope") or {}
    scope_option = str(scope_answer.get("option_id") or "a").strip().lower()
    scope_class = FEATURE_SCOPE_BY_OPTION.get(scope_option, "narrow_pilot")
    scope_text = _answer_text_value(scope_answer) or str(scope_answer.get("option_label") or "").strip()

    acceptance_answer = answers.get("feat_acceptance") or {}
    acceptance_option = str(acceptance_answer.get("option_id") or "a").strip().lower()
    acceptance_method = FEATURE_ACCEPTANCE_BY_OPTION.get(acceptance_option, "manual_proof")
    acceptance_label = str(acceptance_answer.get("option_label") or "Manual proof").strip()

    repo_focus = None
    repo_answer = answers.get("feat_repo") or {}
    if repo_answer and not repo_answer.get("skipped"):
        repo_focus = _answer_text_value(repo_answer) or str(repo_answer.get("option_label") or "").strip() or None

    constraints: list[str] = []
    non_goals = _scope_exclusion_from_answer(answers.get("feat_constraints"))

    feature_title = _feature_title_from_outcome(
        outcome,
        fallback=str(project_context.get("project_name") or "Feature").strip() or "Feature",
    )

    acceptance_statement = f"{feature_title}: {acceptance_label.lower()} confirms the feature works."
    if acceptance_method == "automated_tests":
        verification_detail = "Run the most relevant project test target for this feature."
    elif acceptance_method == "operational_evidence":
        verification_detail = "Confirm logs, state, or diagnostics prove the feature path works."
    else:
        verification_detail = "Felipe completes the feature flow once and confirms the result."

    goals = [outcome]
    if scope_text:
        goals.append(f"Scope: {scope_text}")

    assumptions = []
    if project_context.get("project_name"):
        assumptions.append(f"Project: {project_context.get('project_name')}")
    if vision_seed:
        assumptions.append(f"Project vision: {vision_seed}")
    if repo_focus:
        assumptions.append(f"Repo focus: {repo_focus}")

    brief = {
        "feature_title": feature_title,
        "outcome": outcome,
        "scope_class": scope_class,
        "scope": scope_text or scope_class.replace("_", " "),
        "acceptance_method": acceptance_method,
        "repo_focus": repo_focus,
        "constraints": constraints,
        "non_goals": non_goals,
        "refined_vision": outcome,
        "goals": goals[:8],
        "assumptions": assumptions[:8],
        "acceptance_criteria": [{
            "statement": acceptance_statement,
            "verification_method": "manual" if acceptance_method == "manual_proof" else acceptance_method,
            "verification_detail": verification_detail,
            "machine_checkable": acceptance_method == "automated_tests",
        }],
        "risk_notes": [],
        "open_questions": [],
        "suggested_next_action": "Review the feature brief, then create and approve a Dev execution plan.",
    }
    return brief, None


def _is_placeholder_discovery_brief(brief: str) -> bool:
    text = str(brief or "").strip()
    if not text:
        return True
    if text == PROJECT_DISCOVERY_VISION_SEED:
        return True
    if text.startswith("Project discovery for "):
        return True
    if text.startswith("Project setup for "):
        return True
    return len(text) < DISCOVERY_MIN_NARRATIVE_CHARS


def _discovery_kickoff_questions(
    *,
    project_context: Optional[Dict[str, Any]] = None,
) -> list[Dict[str, Any]]:
    project_name = str((project_context or {}).get("project_name") or "").strip() or "this project"
    return [
        {
            "question_id": DISCOVERY_KICKOFF_QUESTION_ID,
            "prompt": (
                f"Before anything else — what's on your mind for {project_name}? "
                "Describe the problem, idea, or vision in your own words."
            ),
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Discovery starts with your story, not pre-written assumptions.",
            "options": [
                {
                    "option_id": "a",
                    "label": "Use the text field",
                    "description": "Share freely; follow-up questions will build on what you write.",
                },
                {
                    "option_id": "b",
                    "label": "Rough idea is fine",
                    "description": "Partial thoughts are enough to start — we'll clarify together.",
                },
            ],
        },
    ]


def _discovery_context_for_llm(payload: Dict[str, Any]) -> Dict[str, Any]:
    answers = payload.get("answers") or []
    answered_question_ids = {
        item.get("question_id")
        for item in answers
        if item.get("question_id") and not item.get("skipped")
    }
    return {
        "vision_brief": payload.get("vision_brief"),
        "initial_narrative": payload.get("initial_narrative") or payload.get("vision_brief"),
        "project_context": payload.get("project_context") or {},
        "answers": _answers_context(payload),
        "questions_asked": [item.get("prompt") for item in payload.get("questions") or []],
        "grounding": payload.get("grounding") or {},
        "answered_count": len(answered_question_ids),
        "max_questions": DISCOVERY_MAX_QUESTIONS,
        "min_answers_before_ready": DISCOVERY_MIN_ANSWERS,
    }


def _bootstrap_discovery_from_narrative(
    *,
    store: DevClarificationStore,
    clarification_id: str,
    payload: Dict[str, Any],
    force_fallback: bool = False,
) -> Dict[str, Any]:
    if force_fallback:
        advance = _fallback_discovery_advance(payload, answered_count=0)
    else:
        try:
            advance = _generate_discovery_advance(payload)
        except Exception:
            advance = _fallback_discovery_advance(payload, answered_count=0)

    if str(advance.get("action") or "").strip().lower() == "continue" and advance.get("question"):
        question = _validate_discovery_question(
            advance["question"],
            payload=payload,
            question_index=1,
        )
        return store.update(clarification_id, {
            "questions": [question],
            "discovery_turn": 1,
        })

    fallback = _fallback_discovery_advance(payload, answered_count=0)
    if fallback.get("question"):
        question = _validate_discovery_question(
            fallback["question"],
            payload=payload,
            question_index=1,
        )
        return store.update(clarification_id, {
            "questions": [question],
            "discovery_turn": 1,
        })
    raise ValueError("Could not generate first discovery question from narrative")


def _advance_discovery_session(
    *,
    store: DevClarificationStore,
    clarification_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    questions = list(payload.get("questions") or [])
    answers = payload.get("answers") or []
    answered_count = len({
        item.get("question_id")
        for item in answers
        if item.get("question_id") and not item.get("skipped")
    })
    updates: Dict[str, Any] = {
        "discovery_turn": int(payload.get("discovery_turn") or 0) + 1,
    }
    if len(questions) >= DISCOVERY_MAX_QUESTIONS:
        updates["discovery_ready"] = True
        updates["discovery_ready_reason"] = "Maximum discovery questions reached."
        return store.update(clarification_id, updates)

    try:
        advance = _generate_discovery_advance(payload)
    except Exception:
        advance = _fallback_discovery_advance(payload, answered_count=answered_count)

    action = str(advance.get("action") or "continue").strip().lower()
    if action == "ready" and answered_count >= DISCOVERY_MIN_ANSWERS:
        updates["discovery_ready"] = True
        updates["discovery_ready_reason"] = str(advance.get("reason") or "").strip() or "Discovery facilitator marked session ready."
        return store.update(clarification_id, updates)

    if action == "ready" and answered_count < DISCOVERY_MIN_ANSWERS:
        advance = _fallback_discovery_advance(payload, answered_count=answered_count)
        action = str(advance.get("action") or "continue").strip().lower()

    if action == "continue" and advance.get("question"):
        try:
            question = _validate_discovery_question(
                advance["question"],
                payload=payload,
                question_index=len(questions) + 1,
            )
            questions.append(question)
            updates["questions"] = questions
        except Exception:
            fallback = _fallback_discovery_advance(payload, answered_count=answered_count)
            if fallback.get("action") == "ready" and answered_count >= DISCOVERY_MIN_ANSWERS:
                updates["discovery_ready"] = True
                updates["discovery_ready_reason"] = str(fallback.get("reason") or "").strip() or "Fallback marked session ready."
            elif fallback.get("question"):
                question = _validate_discovery_question(
                    fallback["question"],
                    payload=payload,
                    question_index=len(questions) + 1,
                )
                questions.append(question)
                updates["questions"] = questions
    elif answered_count >= DISCOVERY_MIN_ANSWERS and len(questions) >= DISCOVERY_MIN_ANSWERS:
        updates["discovery_ready"] = True
        updates["discovery_ready_reason"] = "Enough discovery answers collected."

    return store.update(clarification_id, updates)


def _generate_discovery_advance(payload: Dict[str, Any]) -> Dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "You are a curious project discovery facilitator. The user shared an opening narrative — "
                "treat it as the source of truth and build on it. After each answer, either ask ONE follow-up "
                "question or mark the session ready to synthesize a discovery brief. "
                "Return JSON: {\"action\":\"continue\",\"question\":{...}} OR {\"action\":\"ready\",\"reason\":\"...\"}. "
                "Each follow-up must react to what the user already said: probe gaps, ask for evidence, clarify "
                "vision, challenge vague language, or explore scope — never generic questionnaire items. "
                "Do not repeat topics already covered in the opening narrative or prior answers. "
                "Reference repository grounding only when paths are provided. "
                "Mark ready when problem, vision, success criteria, and first bet are good enough — not perfect."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(_discovery_context_for_llm(payload), ensure_ascii=False),
        },
    ]
    content = _call_discovery_llm(messages, schema=DISCOVERY_ADVANCE_JSON_SCHEMA, name="dev_discovery_advance", task="dev_discovery_question")
    parsed = _extract_json(content)
    if parsed.get("action") == "continue" and not parsed.get("question"):
        raise ValueError("Discovery advance missing question for continue action")
    return parsed


def _fallback_discovery_advance(payload: Dict[str, Any], *, answered_count: int) -> Dict[str, Any]:
    if answered_count >= DISCOVERY_MIN_ANSWERS:
        return {
            "action": "ready",
            "reason": "Enough discovery answers collected.",
        }
    asked_ids = {
        str(item.get("question_id") or "").strip()
        for item in payload.get("questions") or []
    }
    bank = [
        {
            "question_id": "disc_vision",
            "prompt": "What should this project look like at its best in one or two years?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Vision should describe direction, not a feature list.",
            "options": [
                {"option_id": "a", "label": "Durable capability", "description": "A lasting platform or workflow improvement."},
                {"option_id": "b", "label": "Focused product win", "description": "A sharp user-facing outcome we can ship soon."},
                {"option_id": "c", "label": "Operational reliability", "description": "Failures, drift, or toil are materially reduced."},
            ],
        },
        {
            "question_id": "disc_success",
            "prompt": "How will you know this project succeeded?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Success criteria should be observable outcomes, not outputs.",
            "options": [
                {"option_id": "a", "label": "Behavior change", "description": "A workflow becomes faster, clearer, or more reliable."},
                {"option_id": "b", "label": "Quality bar", "description": "Tests, diagnostics, or review gates prove correctness."},
                {"option_id": "c", "label": "Strategic unlock", "description": "Later features become possible because this exists."},
            ],
        },
        {
            "question_id": "disc_scope",
            "prompt": "What should explicitly stay out of scope for the first bet?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "Non-goals keep the first slice bounded.",
            "options": [
                {"option_id": "a", "label": "No broad refactor", "description": "Avoid platform rewrites while proving value."},
                {"option_id": "b", "label": "No automatic execution", "description": "Planning and approval only until explicitly launched."},
                {"option_id": "c", "label": "No research rabbit holes", "description": "Defer deep integrations until the first bet lands."},
            ],
        },
        {
            "question_id": "disc_first_bet",
            "prompt": "What is the smallest first bet worth building?",
            "recommended_option_id": "a",
            "allow_freeform": True,
            "reason": "The first bet bridges discovery to feature planning.",
            "options": [
                {"option_id": "a", "label": "One workflow end-to-end", "description": "Prove a single path completely before expanding."},
                {"option_id": "b", "label": "Control-plane spine", "description": "Ship durable data/API paths before rich UI."},
                {"option_id": "c", "label": "Operator experience", "description": "Make the planning or review loop feel complete first."},
            ],
        },
    ]
    for idx, template in enumerate(bank, start=1):
        if template["question_id"] not in asked_ids:
            question = dict(template)
            question["question_id"] = _next_discovery_question_id(payload, fallback_index=idx)
            return {"action": "continue", "question": question}
    return {
        "action": "ready",
        "reason": "Fallback bank exhausted; enough discovery context collected.",
    } if answered_count >= DISCOVERY_MIN_ANSWERS else {
        "action": "continue",
        "question": {
            **bank[0],
            "question_id": _next_discovery_question_id(payload, fallback_index=99),
        },
    }


def _validate_discovery_question(
    raw: Dict[str, Any],
    *,
    payload: Dict[str, Any],
    question_index: int,
) -> Dict[str, Any]:
    validated = _validate_questions({"questions": [raw]}, max_questions=1, min_questions=1)
    question = validated[0]
    question["question_id"] = _next_discovery_question_id(payload, fallback_index=question_index)
    return question


def _next_discovery_question_id(payload: Dict[str, Any], *, fallback_index: int) -> str:
    existing = {
        str(item.get("question_id") or "").strip()
        for item in payload.get("questions") or []
    }
    candidate = f"disc_{fallback_index}"
    if candidate not in existing:
        return candidate
    return f"disc_{fallback_index}_{len(existing) + 1}"


def _next_discovery_question_id_from_raw(raw_id: Any, *, question_index: int) -> str:
    question_id = str(raw_id or "").strip() or f"disc_{question_index}"
    if not question_id.startswith("disc_"):
        return f"disc_{question_index}"
    return question_id


def _call_discovery_llm(
    messages: list[Dict[str, str]],
    *,
    schema: Dict[str, Any],
    name: str,
    task: str,
) -> str:
    kwargs = {
        "task": task,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 2200,
        "timeout": 55,
    }
    try:
        response = call_llm(
            **kwargs,
            extra_body={
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": name,
                        "schema": schema,
                        "strict": True,
                    },
                },
            },
        )
    except Exception as exc:
        error_text = str(exc).lower()
        if "response_format" not in error_text and "json_schema" not in error_text and "unsupported" not in error_text:
            raise
        response = call_llm(**kwargs)
    return str(response.choices[0].message.content or "").strip()


def _build_discovery_brief(payload: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[str]]:
    try:
        brief = _validate_discovery_brief(_generate_discovery_brief_with_llm(payload))
        return brief, None
    except Exception as exc:
        fallback = _fallback_discovery_brief(payload)
        return fallback, f"Discovery brief synthesis failed; using deterministic fallback: {exc}"


def _generate_discovery_brief_with_llm(payload: Dict[str, Any]) -> Dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "Synthesize a project discovery brief from discovery answers, project context, and read-only "
                "repository grounding. Return only valid JSON matching the schema. "
                "Write a sharp problem statement (WHO/WHAT/WHY), a directional vision, measurable success criteria, "
                "explicit scope boundaries, assumptions, risks, open questions, and a concrete first bet. "
                "Reference only file paths present in grounding. Do not invent repositories."
            ),
        },
        {
            "role": "user",
            "content": json.dumps({
                "vision_brief": payload.get("vision_brief"),
                "initial_narrative": payload.get("initial_narrative") or payload.get("vision_brief"),
                "project_context": payload.get("project_context") or {},
                "answers": _answers_context(payload),
                "grounding": payload.get("grounding") or {},
                "revision_feedback": payload.get("revision_feedback"),
                "discovery_ready_reason": payload.get("discovery_ready_reason"),
            }, ensure_ascii=False),
        },
    ]
    content = _call_discovery_llm(messages, schema=DISCOVERY_BRIEF_JSON_SCHEMA, name="dev_discovery_brief", task="dev_discovery_synthesis")
    try:
        return _extract_json(content)
    except Exception:
        repair_messages = [
            messages[0],
            {"role": "user", "content": f"Repair this into valid discovery brief schema JSON only:\n{content}"},
        ]
        repaired = _call_discovery_llm(repair_messages, schema=DISCOVERY_BRIEF_JSON_SCHEMA, name="dev_discovery_brief", task="dev_discovery_synthesis")
        return _extract_json(repaired)


def _validate_discovery_brief(value: Dict[str, Any]) -> Dict[str, Any]:
    repositories: list[Dict[str, Any]] = []
    for item in value.get("repositories") or []:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip()
        if not path:
            continue
        repositories.append({
            "label": str(item.get("label") or "").strip() or None,
            "path": path,
        })
    return {
        "discovery_brief_version": int(value.get("discovery_brief_version") or 1),
        "project_name": str(value.get("project_name") or "").strip() or "New Project",
        "problem": str(value.get("problem") or "").strip(),
        "problem_evidence": _string_list(value.get("problem_evidence"))[:8],
        "vision": str(value.get("vision") or "").strip(),
        "success_criteria": _string_list(value.get("success_criteria"))[:8],
        "users_operators": _string_list(value.get("users_operators"))[:8],
        "scope_in": _string_list(value.get("scope_in"))[:8],
        "scope_out": _string_list(value.get("scope_out"))[:8],
        "parking_lot": _string_list(value.get("parking_lot"))[:8],
        "assumptions": _string_list(value.get("assumptions"))[:8],
        "risks": _string_list(value.get("risks"))[:8],
        "open_questions": _string_list(value.get("open_questions"))[:8],
        "first_bet": str(value.get("first_bet") or "").strip(),
        "repositories": repositories[:10],
        "constraints": _string_list(value.get("constraints"))[:8],
        "non_goals": _string_list(value.get("non_goals"))[:8],
        "intent_class": str(value.get("intent_class") or "existing_codebase").strip(),
        "suggested_next_action": str(value.get("suggested_next_action") or "").strip()
        or "Review the discovery brief, approve it, then plan the first feature.",
    }


def _fallback_discovery_brief(payload: Dict[str, Any]) -> Dict[str, Any]:
    answers = _answers_context(payload)
    project_context = payload.get("project_context") or {}
    default_name = str(project_context.get("project_name") or "").strip() or "New Project"
    answered_text = [
        str(item.get("answer") or "").strip()
        for item in answers
        if not item.get("skipped") and str(item.get("answer") or "").strip()
    ]
    problem = answered_text[0] if answered_text else str(payload.get("vision_brief") or PROJECT_DISCOVERY_VISION_SEED)
    vision = answered_text[1] if len(answered_text) > 1 else problem
    repositories = []
    for repo in (project_context.get("repositories") or []):
        if isinstance(repo, dict) and str(repo.get("path") or "").strip():
            repositories.append({
                "label": str(repo.get("label") or "").strip() or None,
                "path": str(repo.get("path") or "").strip(),
            })
    return {
        "discovery_brief_version": 1,
        "project_name": default_name,
        "problem": problem,
        "problem_evidence": answered_text[:3],
        "vision": vision,
        "success_criteria": ["Felipe can explain who, problem, vision, and first bet without chat history."],
        "users_operators": ["Felipe as operator"],
        "scope_in": answered_text[2:4],
        "scope_out": ["Broad platform rewrite", "Automatic execution without approval"],
        "parking_lot": [],
        "assumptions": [f"Project: {default_name}"] if default_name else [],
        "risks": ["Discovery brief synthesized without LLM; validate before approving."],
        "open_questions": [item.get("question") for item in answers if item.get("skipped")][:8],
        "first_bet": answered_text[-1] if answered_text else "Plan the first feature slice.",
        "repositories": repositories,
        "constraints": [],
        "non_goals": ["Do not launch workers from discovery alone."],
        "intent_class": "existing_codebase",
        "suggested_next_action": "Review the discovery brief, approve it, then plan the first feature.",
    }


def _validate_questions(
    payload: Dict[str, Any],
    *,
    max_questions: int,
    min_questions: int = MIN_TARGET_QUESTIONS,
) -> list[Dict[str, Any]]:
    raw_questions = payload.get("questions")
    if not isinstance(raw_questions, list):
        raise ValueError("LLM response missing questions[]")
    questions: list[Dict[str, Any]] = []
    for idx, raw in enumerate(raw_questions[:max_questions], start=1):
        if not isinstance(raw, dict):
            continue
        prompt = str(raw.get("prompt") or "").strip()
        options = raw.get("options") or []
        if not prompt or not isinstance(options, list):
            continue
        normalized_options = []
        for option_idx, option in enumerate(options[:4], start=1):
            if not isinstance(option, dict):
                continue
            option_id = str(option.get("option_id") or chr(96 + option_idx)).strip()
            label = str(option.get("label") or "").strip()
            description = str(option.get("description") or "").strip()
            if label and description:
                normalized_options.append({
                    "option_id": option_id,
                    "label": label,
                    "description": description,
                })
        if len(normalized_options) < 2:
            continue
        recommended = str(raw.get("recommended_option_id") or normalized_options[0]["option_id"]).strip()
        if recommended not in {option["option_id"] for option in normalized_options}:
            recommended = normalized_options[0]["option_id"]
        questions.append({
            "question_id": str(raw.get("question_id") or f"q{idx}").strip(),
            "prompt": prompt,
            "recommended_option_id": recommended,
            "allow_freeform": bool(raw.get("allow_freeform", True)),
            "reason": str(raw.get("reason") or "").strip(),
            "options": normalized_options,
        })
    if len(questions) < min_questions:
        raise ValueError(f"LLM response did not contain at least {min_questions} valid questions")
    return questions


def _find_option(question: Dict[str, Any], option_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not option_id:
        return None
    for option in question.get("options") or []:
        if option.get("option_id") == option_id:
            return option
    raise ValueError(f"Unknown option_id for current question: {option_id}")


def _build_clarified_brief(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        brief = _validate_clarified_brief(_generate_clarified_brief_with_llm(payload))
        return _validate_clarified_brief_criteria(brief, payload)
    except Exception as exc:
        fallback = _fallback_clarified_brief(payload)
        fallback["warning"] = f"LLM clarified brief synthesis failed; using deterministic fallback brief: {exc}"
        return fallback


def _generate_clarified_brief_with_llm(payload: Dict[str, Any]) -> Dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "Synthesize a clarified software/product spec brief from the user's vision, "
                "clarification answers, project context, and read-only repository grounding. "
                "Return only valid JSON matching the provided schema. "
                "Make acceptance criteria concrete and verifiable. verification_detail for any "
                "machine_checkable: true criterion MUST match one of these exact command shapes: "
                f"{'; '.join(ALLOWED_VERIFICATION_COMMAND_SHAPES)}. "
                "Reference only files/paths present in the provided repository grounding; do not invent file paths. "
                "Prefer a whole-suite or directory-level command when unsure of an exact file. "
                "If no allowlisted command fits, set machine_checkable: false and describe a manual check instead. "
                "Do not create worker tasks, launch work, approve execution, or claim implementation has started."
            ),
        },
        {
            "role": "user",
            "content": json.dumps({
                "vision_brief": payload.get("vision_brief"),
                "project_context": payload.get("project_context") or {},
                "answers": _answers_context(payload),
                "grounding": payload.get("grounding") or {},
                "grounding_provenance": payload.get("grounding_provenance") or [],
                "grounding_warnings": payload.get("grounding_warnings") or [],
                "repository_grounding_paths": _grounding_paths(payload.get("grounding_provenance")),
            }, ensure_ascii=False),
        },
    ]
    content = _call_brief_llm(messages)
    try:
        return _extract_json(content)
    except Exception:
        repair_messages = [
            messages[0],
            {"role": "user", "content": f"Repair this into valid clarified brief schema JSON only:\n{content}"},
        ]
        repaired = _call_brief_llm(repair_messages)
        return _extract_json(repaired)


def _call_brief_llm(messages: list[Dict[str, str]]) -> str:
    kwargs = {
        "task": "dev_clarification_synthesis",
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 3200,
        "timeout": 55,
    }
    try:
        response = call_llm(
            **kwargs,
            extra_body={
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "dev_clarified_brief",
                        "schema": CLARIFIED_BRIEF_JSON_SCHEMA,
                        "strict": True,
                    },
                },
            },
        )
    except Exception as exc:
        error_text = str(exc).lower()
        if "response_format" not in error_text and "json_schema" not in error_text and "unsupported" not in error_text:
            raise
        response = call_llm(**kwargs)
    return str(response.choices[0].message.content or "").strip()


def _validate_clarified_brief(value: Dict[str, Any]) -> Dict[str, Any]:
    criteria = normalize_acceptance_criteria(value.get("acceptance_criteria"))
    if not criteria:
        raise ValueError("Clarified brief requires acceptance_criteria")
    return {
        "refined_vision": str(value.get("refined_vision") or "").strip(),
        "goals": _string_list(value.get("goals"))[:8],
        "non_goals": _string_list(value.get("non_goals"))[:8],
        "constraints": _string_list(value.get("constraints"))[:8],
        "assumptions": _string_list(value.get("assumptions"))[:8],
        "acceptance_criteria": criteria[:8],
        "risk_notes": _string_list(value.get("risk_notes"))[:8],
        "open_questions": _string_list(value.get("open_questions"))[:8],
        "suggested_next_action": str(value.get("suggested_next_action") or "").strip()
        or "Review the clarified brief, then draft a Dev execution plan if the direction is correct.",
    }


def _validate_clarified_brief_criteria(brief: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    criteria, warnings = validate_and_downgrade_criteria(
        brief.get("acceptance_criteria"),
        repo_roots=_repo_roots_from_project_context(payload.get("project_context") or {}),
    )
    if not warnings:
        return brief
    brief = dict(brief)
    brief["acceptance_criteria"] = criteria[:8]
    brief["warning"] = _combine_warning(
        brief.get("warning"),
        "Acceptance criteria downgraded: " + " ".join(warnings),
    )
    return brief


def _fallback_clarified_brief(payload: Dict[str, Any]) -> Dict[str, Any]:
    answers = payload.get("answers") or []
    project_context = payload.get("project_context") or {}
    answered = [item for item in answers if not item.get("skipped")]
    skipped = [item for item in answers if item.get("skipped")]
    goals = []
    constraints = []
    assumptions = []
    if project_context.get("project_name"):
        assumptions.append(f"Project: {project_context.get('project_name')}")
    if project_context.get("vision"):
        assumptions.append(f"Project vision: {project_context.get('vision')}")
    for item in answered:
        text = item.get("answer_text") or item.get("option_label")
        if text:
            goals.append(f"{item.get('question_prompt')}: {text}")
            assumptions.append(f"Felipe selected: {text}")
    for item in skipped:
        constraints.append(f"Clarify later: {item.get('question_prompt')}")
    return {
        "refined_vision": payload.get("vision_brief"),
        "goals": goals[:8],
        "non_goals": ["Do not create or launch a Dev execution plan from this clarification alone."],
        "constraints": constraints,
        "assumptions": assumptions[:8],
        "acceptance_criteria": [
            {
                "statement": "A human can turn this clarified brief into an implementation plan.",
                "verification_method": "manual",
                "verification_detail": "Review the clarified brief before creating a plan artifact.",
                "machine_checkable": False,
            },
            {
                "statement": "Open questions are explicit before any worker execution begins.",
                "verification_method": "manual",
                "verification_detail": "Confirm skipped questions are listed in open_questions.",
                "machine_checkable": False,
            },
        ],
        "risk_notes": ["This is planning guidance only; technical feasibility still needs codebase validation."],
        "open_questions": [item.get("question_prompt") for item in skipped],
        "suggested_next_action": "Review the clarified brief, then draft a Dev execution plan if the direction is correct.",
    }


def _answers_context(payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    questions_by_id = {
        item.get("question_id"): item
        for item in payload.get("questions") or []
    }
    answers = []
    for item in payload.get("answers") or []:
        question = questions_by_id.get(item.get("question_id")) or {}
        answers.append({
            "question_id": item.get("question_id"),
            "question": item.get("question_prompt") or question.get("prompt"),
            "answer": item.get("answer_text") or item.get("option_label"),
            "skipped": bool(item.get("skipped")),
        })
    return answers


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _combine_warning(*values: Any) -> Optional[str]:
    parts = [str(value).strip() for value in values if str(value or "").strip()]
    return " ".join(parts) if parts else None


def _repo_roots_from_project_context(project_context: Dict[str, Any]) -> list[str]:
    roots: list[str] = []
    repositories = project_context.get("repositories")
    if isinstance(repositories, list):
        for repo in repositories:
            if isinstance(repo, dict):
                path = str(repo.get("path") or "").strip()
                if path:
                    roots.append(path)
    return roots


def _grounding_paths(provenance: Any) -> list[str]:
    paths: list[str] = []
    if not isinstance(provenance, list):
        return paths
    for item in provenance:
        if isinstance(item, str):
            path = item.strip()
            if path:
                paths.append(path)
            continue
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or item.get("repo_path") or "").strip()
        if path:
            paths.append(path)
    return paths


def _normalize_project_context(
    value: Optional[Dict[str, Any]],
    *,
    project_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    repositories = []
    for item in value.get("repositories") or []:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip()
        if not path:
            continue
        repositories.append({
            "label": str(item.get("label") or "").strip() or None,
            "path": path,
        })
    work_items = [
        str(item).strip()
        for item in (value.get("work_items") or [])
        if str(item).strip()
    ]
    context = {
        "project_id": str(value.get("project_id") or project_id or "").strip() or None,
        "project_name": str(value.get("project_name") or "").strip() or None,
        "vision": str(value.get("vision") or "").strip() or None,
        "coordinator_profile": str(value.get("coordinator_profile") or "").strip() or None,
        "repositories": repositories[:10],
        "work_items": work_items[:20],
    }
    return context if any(context.get(key) for key in ("project_id", "project_name", "vision", "coordinator_profile")) or repositories or work_items else None
