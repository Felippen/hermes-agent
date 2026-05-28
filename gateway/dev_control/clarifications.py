"""Durable Dev clarification sessions for planning-mode vision refinement."""

from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from agent.auxiliary_client import call_llm
from hermes_state import DEFAULT_DB_PATH, apply_wal_with_fallback


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dev_clarification_sessions (
    clarification_id TEXT PRIMARY KEY,
    project_id TEXT,
    session_id TEXT,
    status TEXT NOT NULL,
    vision_brief TEXT NOT NULL,
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

DEFAULT_PROJECT_ID = "OrynWorkspace"
DEFAULT_MAX_QUESTIONS = 5
MIN_TARGET_QUESTIONS = 3
MAX_QUESTION_LIMIT = 5
CLARIFICATION_STATUSES = {"active", "completed", "cancelled", "expired"}

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
                    current_question_index, questions, answers, clarified_brief,
                    created_at, updated_at, completed_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    current_question_index = ?, questions = ?, answers = ?,
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
    max_questions: int = DEFAULT_MAX_QUESTIONS,
) -> Dict[str, Any]:
    brief = str(vision_brief or "").strip()
    if not brief:
        raise ValueError("vision_brief is required")
    question_count = max(MIN_TARGET_QUESTIONS, min(int(max_questions or DEFAULT_MAX_QUESTIONS), MAX_QUESTION_LIMIT))
    generation_mode = "llm"
    warning = None
    try:
        questions = _generate_questions(brief, max_questions=question_count)
    except Exception as exc:
        generation_mode = "fallback"
        warning = f"LLM question generation failed; using deterministic fallback questions: {exc}"
        questions = _fallback_questions(max_questions=question_count)
    payload = {
        "object": "hermes.dev_clarification",
        "clarification_id": f"devclar-{uuid.uuid4().hex[:10]}",
        "project_id": project_id or DEFAULT_PROJECT_ID,
        "session_id": session_id,
        "status": "active",
        "vision_brief": brief,
        "current_question_index": 0,
        "questions": questions,
        "answers": [],
        "clarified_brief": None,
        "completed_at": None,
        "generation_mode": generation_mode,
        "warning": warning,
    }
    return _with_current_question(store.create(payload))


def list_clarifications(
    *,
    store: DevClarificationStore,
    project_id: Optional[str] = None,
    session_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    data = [_with_current_question(item) for item in store.list(
        project_id=project_id,
        session_id=session_id,
        status=status,
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
    return _with_current_question(store.update(clarification_id, {
        "answers": answers,
        "current_question_index": next_index,
    }))


def complete_clarification(*, store: DevClarificationStore, clarification_id: str) -> Dict[str, Any]:
    payload = get_clarification(store=store, clarification_id=clarification_id)
    if payload["status"] not in {"active", "completed"}:
        raise ValueError(f"Clarification session is {payload['status']} and cannot be completed")
    clarified = _build_clarified_brief(payload)
    return _with_current_question(store.update(clarification_id, {
        "status": "completed",
        "current_question_index": len(payload.get("questions") or []),
        "clarified_brief": clarified,
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


def _row_values(payload: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["clarification_id"],
        payload.get("project_id"),
        payload.get("session_id"),
        payload["status"],
        payload["vision_brief"],
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
    payload.update({
        "object": "hermes.dev_clarification",
        "clarification_id": row["clarification_id"],
        "project_id": row["project_id"],
        "session_id": row["session_id"],
        "status": row["status"],
        "vision_brief": row["vision_brief"],
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
    payload["current_question"] = questions[index] if 0 <= index < len(questions) and payload.get("status") == "active" else None
    minimum_answers = min(len(questions), MIN_TARGET_QUESTIONS)
    answered_question_ids = {
        item.get("question_id")
        for item in (payload.get("answers") or [])
        if item.get("question_id")
    }
    payload["can_complete"] = len(answered_question_ids) >= minimum_answers or index >= minimum_answers
    return payload


def _generate_questions(vision_brief: str, *, max_questions: int) -> list[Dict[str, Any]]:
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
            "content": f"Vision brief:\n{vision_brief}\n\nMax questions: {max_questions}",
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


def _validate_questions(payload: Dict[str, Any], *, max_questions: int) -> list[Dict[str, Any]]:
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
    if len(questions) < MIN_TARGET_QUESTIONS:
        raise ValueError("LLM response did not contain at least three valid questions")
    return questions


def _find_option(question: Dict[str, Any], option_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not option_id:
        return None
    for option in question.get("options") or []:
        if option.get("option_id") == option_id:
            return option
    raise ValueError(f"Unknown option_id for current question: {option_id}")


def _build_clarified_brief(payload: Dict[str, Any]) -> Dict[str, Any]:
    answers = payload.get("answers") or []
    answered = [item for item in answers if not item.get("skipped")]
    skipped = [item for item in answers if item.get("skipped")]
    goals = []
    constraints = []
    assumptions = []
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
            "A human can turn this clarified brief into an implementation plan.",
            "Open questions are explicit before any worker execution begins.",
        ],
        "risk_notes": ["This is planning guidance only; technical feasibility still needs codebase validation."],
        "open_questions": [item.get("question_prompt") for item in skipped],
        "suggested_next_action": "Review the clarified brief, then draft a Dev execution plan if the direction is correct.",
    }
