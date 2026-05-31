"""Tests for project_discovery clarification kind."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from gateway.dev_control.clarifications import (
    DISCOVERY_KICKOFF_QUESTION_ID,
    DevClarificationStore,
    answer_clarification,
    approve_clarification_brief,
    complete_clarification,
    get_clarification,
    revise_clarification_brief,
    start_clarification,
)


def _discovery_advance_continue() -> str:
    return """
    {
      "action": "continue",
      "question": {
        "question_id": "disc_vision",
        "prompt": "What is the north-star vision?",
        "recommended_option_id": "a",
        "allow_freeform": true,
        "reason": "Vision sets direction.",
        "options": [
          {"option_id": "a", "label": "Durable platform", "description": "Long-lived capability."},
          {"option_id": "b", "label": "Focused win", "description": "Ship one sharp outcome."}
        ]
      }
    }
    """


def _discovery_advance_ready() -> str:
    return '{"action": "ready", "reason": "Problem, vision, and first bet are clear enough."}'


def _discovery_brief_response() -> str:
    return """
    {
      "discovery_brief_version": 1,
      "project_name": "Alpha Project",
      "problem": "Planning drifts because project intent is shallow.",
      "problem_evidence": ["Felipe re-explains context every session"],
      "vision": "Dev runs projects with durable intent.",
      "success_criteria": ["Vision goal reads like a brief, not one sentence"],
      "users_operators": ["Felipe"],
      "scope_in": ["Interactive discovery session"],
      "scope_out": ["Automatic execution"],
      "parking_lot": ["Vault research"],
      "assumptions": ["Hermes clarify can branch adaptively"],
      "risks": ["LLM follow-ups feel generic"],
      "open_questions": [],
      "first_bet": "Ship project discovery session",
      "repositories": [],
      "constraints": [],
      "non_goals": ["No worker launch from discovery"],
      "intent_class": "existing_codebase",
      "suggested_next_action": "Approve the brief, then plan the first feature."
    }
    """


def _answer_discovery(store: DevClarificationStore, clarification_id: str, count: int = 4) -> None:
    session = store.get(clarification_id)
    assert session is not None
    for index in range(count):
        questions = session.get("questions") or []
        current_index = min(int(session.get("current_question_index") or 0), len(questions) - 1)
        question = questions[current_index]
        session = answer_clarification(
            store=store,
            clarification_id=clarification_id,
            question_id=question["question_id"],
            answer_text=f"Answer {index + 1} for discovery.",
        )


def test_project_discovery_start_with_placeholder_returns_kickoff_only(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="",
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        assert session["clarification_kind"] == "project_discovery"
        assert session["generation_mode"] == "adaptive"
        assert len(session["questions"]) == 1
        assert session["questions"][0]["question_id"] == DISCOVERY_KICKOFF_QUESTION_ID
    finally:
        store.close()


@patch("gateway.dev_control.clarifications._call_discovery_llm")
def test_project_discovery_start_from_narrative_generates_first_follow_up(mock_llm, tmp_path):
    mock_llm.return_value = _discovery_advance_continue()
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        narrative = (
            "Planning keeps drifting because project intent stays shallow. "
            "I want Dev to run projects with durable context."
        )
        session = start_clarification(
            store=store,
            vision_brief=narrative,
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        assert session["initial_narrative"] == narrative
        assert len(session["questions"]) == 1
        assert session["questions"][0]["prompt"] == "What is the north-star vision?"
        mock_llm.assert_called_once()
    finally:
        store.close()


@patch("gateway.dev_control.clarifications._call_discovery_llm")
def test_project_discovery_kickoff_answer_appends_follow_up(mock_llm, tmp_path):
    mock_llm.return_value = _discovery_advance_continue()
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="",
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        session = answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id=DISCOVERY_KICKOFF_QUESTION_ID,
            answer_text="Planning keeps drifting from intent because we never capture durable project context.",
        )
        assert session["initial_narrative"] == session["vision_brief"]
        assert len(session["questions"]) == 2
        assert session["questions"][-1]["prompt"] == "What is the north-star vision?"
    finally:
        store.close()


@patch("gateway.dev_control.clarifications._call_discovery_llm")
def test_project_discovery_ready_sets_can_complete(mock_llm, tmp_path):
    mock_llm.side_effect = [
        _discovery_advance_continue(),
        _discovery_advance_continue(),
        _discovery_advance_continue(),
        _discovery_advance_ready(),
        _discovery_advance_ready(),
    ]
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        narrative = (
            "Planning keeps drifting because project intent stays shallow. "
            "I want Dev to run projects with durable context."
        )
        session = start_clarification(
            store=store,
            vision_brief=narrative,
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        _answer_discovery(store, session["clarification_id"], count=4)
        session = get_clarification(store=store, clarification_id=session["clarification_id"])
        assert session.get("discovery_ready") is True
        assert session.get("can_complete") is True
    finally:
        store.close()


@patch("gateway.dev_control.clarifications._call_discovery_llm")
def test_project_discovery_complete_sets_brief_ready(mock_llm, tmp_path):
    mock_llm.side_effect = [
        _discovery_advance_continue(),
        _discovery_advance_continue(),
        _discovery_advance_continue(),
        _discovery_advance_ready(),
        _discovery_advance_ready(),
        _discovery_brief_response(),
    ]
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        narrative = (
            "Planning keeps drifting because project intent stays shallow. "
            "I want Dev to run projects with durable context."
        )
        session = start_clarification(
            store=store,
            vision_brief=narrative,
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        clarification_id = session["clarification_id"]
        _answer_discovery(store, clarification_id, count=4)
        completed = complete_clarification(store=store, clarification_id=clarification_id)
        assert completed["status"] == "brief_ready"
        assert completed.get("completed_at") is None
        brief = completed["clarified_brief"]
        assert brief["project_name"] == "Alpha Project"
        assert brief["discovery_brief_version"] == 1
        assert brief["first_bet"]
    finally:
        store.close()


@patch("gateway.dev_control.clarifications._call_discovery_llm")
def test_project_discovery_approve_and_revise(mock_llm, tmp_path):
    mock_llm.side_effect = [
        _discovery_advance_continue(),
        _discovery_advance_continue(),
        _discovery_advance_continue(),
        _discovery_advance_ready(),
        _discovery_brief_response(),
        _discovery_brief_response(),
    ]
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        narrative = (
            "Planning keeps drifting because project intent stays shallow. "
            "I want Dev to run projects with durable context."
        )
        session = start_clarification(
            store=store,
            vision_brief=narrative,
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        clarification_id = session["clarification_id"]
        _answer_discovery(store, clarification_id, count=4)
        brief_ready = complete_clarification(store=store, clarification_id=clarification_id)
        revised = revise_clarification_brief(
            store=store,
            clarification_id=clarification_id,
            feedback="Make the first bet more concrete.",
        )
        assert revised["status"] == "brief_ready"
        assert revised["clarified_brief"]["first_bet"]
        approved = approve_clarification_brief(store=store, clarification_id=clarification_id)
        assert approved["status"] == "completed"
        assert approved.get("brief_approved_at")
        assert approved.get("completed_at")
    finally:
        store.close()


@patch("gateway.dev_control.clarifications._call_discovery_llm")
def test_project_discovery_reanswer_does_not_append_follow_up(mock_llm, tmp_path):
    mock_llm.side_effect = [_discovery_advance_continue(), _discovery_advance_continue()]
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        narrative = (
            "Planning keeps drifting because project intent stays shallow. "
            "I want Dev to run projects with durable context."
        )
        session = start_clarification(
            store=store,
            vision_brief=narrative,
            project_id="AlphaProject",
            clarification_kind="project_discovery",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        clarification_id = session["clarification_id"]
        first_question = session["questions"][0]
        session = answer_clarification(
            store=store,
            clarification_id=clarification_id,
            question_id=first_question["question_id"],
            answer_text="First answer.",
        )
        assert len(session["questions"]) == 2
        session = answer_clarification(store=store, clarification_id=clarification_id, back=True)
        session = answer_clarification(
            store=store,
            clarification_id=clarification_id,
            question_id=first_question["question_id"],
            answer_text="Revised first answer.",
        )
        assert len(session["questions"]) == 2
        assert mock_llm.call_count == 2
    finally:
        store.close()


def test_project_discovery_rejects_unknown_kind(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        with pytest.raises(ValueError, match="Unsupported clarification_kind"):
            start_clarification(
                store=store,
                vision_brief="Discovery",
                clarification_kind="unknown_kind",
            )
    finally:
        store.close()
