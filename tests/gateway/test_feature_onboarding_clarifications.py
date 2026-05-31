"""Tests for feature_onboarding clarification kind."""

from __future__ import annotations

import sqlite3

from gateway.dev_control.clarifications import (
    DevClarificationStore,
    answer_clarification,
    complete_clarification,
    start_clarification,
)


def _answer_feature(store: DevClarificationStore, clarification_id: str, *, multi_repo: bool = False) -> None:
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="feat_outcome",
        answer_text="Ship feature onboarding from the project dashboard.",
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="feat_scope",
        option_id="a",
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="feat_acceptance",
        option_id="b",
    )
    if multi_repo:
        answer_clarification(
            store=store,
            clarification_id=clarification_id,
            question_id="feat_repo",
            option_id="a",
            answer_text="/Users/felipe/projects/Oryn",
        )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="feat_constraints",
        option_id="a",
    )


def test_feature_onboarding_single_repo_omits_repo_question(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="Improve project planning",
            project_id="OrynWorkspace",
            clarification_kind="feature_onboarding",
            project_context={
                "project_id": "OrynWorkspace",
                "project_name": "Oryn",
                "vision": "Make Dev planning reliable.",
                "repositories": [{"label": "Oryn", "path": "/Users/felipe/projects/Oryn"}],
            },
        )
        question_ids = [question["question_id"] for question in session["questions"]]
        assert question_ids == [
            "feat_outcome",
            "feat_scope",
            "feat_acceptance",
            "feat_constraints",
        ]
    finally:
        store.close()


def test_feature_onboarding_multi_repo_includes_repo_question(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="Improve project planning",
            project_id="OrynWorkspace",
            clarification_kind="feature_onboarding",
            project_context={
                "project_id": "OrynWorkspace",
                "project_name": "Oryn",
                "repositories": [
                    {"label": "App", "path": "/Users/felipe/projects/Oryn/apps/oryn-workspace"},
                    {"label": "Hermes", "path": "/Users/felipe/projects/Oryn/hermes-agent"},
                ],
            },
        )
        assert "feat_repo" in [question["question_id"] for question in session["questions"]]
    finally:
        store.close()


def test_feature_onboarding_complete_builds_planning_brief(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="Improve project planning",
            project_id="OrynWorkspace",
            clarification_kind="feature_onboarding",
            project_context={
                "project_id": "OrynWorkspace",
                "project_name": "Oryn",
                "vision": "Make Dev planning reliable.",
            },
        )
        _answer_feature(store, session["clarification_id"])
        completed = complete_clarification(store=store, clarification_id=session["clarification_id"])
        brief = completed["clarified_brief"]
        assert brief["feature_title"].startswith("Ship feature onboarding")
        assert brief["acceptance_method"] == "automated_tests"
        assert brief["refined_vision"]
        assert brief["acceptance_criteria"]
        assert brief["non_goals"]
    finally:
        store.close()


def test_feature_onboarding_freeform_scope_exclusion_goes_to_non_goals(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="Improve project planning",
            project_id="OrynWorkspace",
            clarification_kind="feature_onboarding",
            project_context={
                "project_id": "OrynWorkspace",
                "project_name": "Oryn",
                "vision": "Make Dev planning reliable.",
            },
        )
        clarification_id = session["clarification_id"]
        answer_clarification(store=store, clarification_id=clarification_id, question_id="feat_outcome", answer_text="Ship feature onboarding.")
        answer_clarification(store=store, clarification_id=clarification_id, question_id="feat_scope", option_id="a")
        answer_clarification(store=store, clarification_id=clarification_id, question_id="feat_acceptance", option_id="b")
        answer_clarification(
            store=store,
            clarification_id=clarification_id,
            question_id="feat_constraints",
            answer_text="No broad refactor in the first slice.",
        )
        completed = complete_clarification(store=store, clarification_id=clarification_id)
        brief = completed["clarified_brief"]
        assert brief["non_goals"] == ["No broad refactor in the first slice."]
        assert brief["constraints"] == []
    finally:
        store.close()
