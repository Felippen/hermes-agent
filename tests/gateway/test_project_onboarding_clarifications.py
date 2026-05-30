"""Tests for project_onboarding clarification kind."""

from __future__ import annotations

import sqlite3

import pytest

from gateway.dev_control.clarifications import (
    DevClarificationStore,
    answer_clarification,
    complete_clarification,
    list_clarifications,
    start_clarification,
)


def _answer_all_onboarding(store: DevClarificationStore, clarification_id: str) -> None:
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="onb_name",
        option_id="a",
        answer_text="Alpha Project",
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="onb_intent",
        option_id="b",
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="onb_vision",
        answer_text="Ship a reliable onboarding flow for new projects.",
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="onb_repo",
        skipped=True,
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="onb_extra_repos",
        option_id="b",
    )
    answer_clarification(
        store=store,
        clarification_id=clarification_id,
        question_id="onb_constraints",
        option_id="a",
    )


def test_project_onboarding_start_returns_deterministic_questions(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="",
            project_id="AlphaProject",
            clarification_kind="project_onboarding",
            project_context={
                "project_id": "AlphaProject",
                "project_name": "Alpha Project",
            },
        )
        assert session["clarification_kind"] == "project_onboarding"
        assert session["generation_mode"] == "deterministic"
        assert [question["question_id"] for question in session["questions"]] == [
            "onb_name",
            "onb_intent",
            "onb_vision",
            "onb_repo",
            "onb_extra_repos",
            "onb_constraints",
        ]
        assert session["questions"][0]["options"][0]["label"] == "Alpha Project"
    finally:
        store.close()


def test_project_onboarding_complete_builds_profile_with_deferred_repo(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        session = start_clarification(
            store=store,
            vision_brief="",
            project_id="AlphaProject",
            clarification_kind="project_onboarding",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        _answer_all_onboarding(store, session["clarification_id"])
        completed = complete_clarification(store=store, clarification_id=session["clarification_id"])
        profile = completed["clarified_brief"]
        assert profile["project_name"] == "Alpha Project"
        assert profile["intent_class"] == "existing_codebase"
        assert "onboarding" in profile["vision"].lower()
        assert profile["repos_deferred"] is True
        assert profile["repositories"] == []
        assert profile["non_goals"]
    finally:
        store.close()


def test_project_onboarding_list_filters_by_kind(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        start_clarification(
            store=store,
            vision_brief="Feature idea",
            project_id="OrynWorkspace",
            clarification_kind="planning",
        )
        start_clarification(
            store=store,
            vision_brief="",
            project_id="OrynWorkspace",
            clarification_kind="project_onboarding",
        )
        onboarding = list_clarifications(
            store=store,
            project_id="OrynWorkspace",
            clarification_kind="project_onboarding",
        )
        planning = list_clarifications(
            store=store,
            project_id="OrynWorkspace",
            clarification_kind="planning",
        )
        assert onboarding["total"] == 1
        assert planning["total"] == 1
        assert onboarding["data"][0]["clarification_kind"] == "project_onboarding"
    finally:
        store.close()


def test_project_onboarding_migrates_legacy_clarification_table(tmp_path):
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE dev_clarification_sessions (
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
        """
    )
    conn.close()

    store = DevClarificationStore(db_path=db_path)
    try:
        session = start_clarification(
            store=store,
            vision_brief="",
            project_id="LegacyProject",
            clarification_kind="project_onboarding",
        )
        assert session["clarification_kind"] == "project_onboarding"
    finally:
        store.close()


def test_project_onboarding_rejects_unknown_kind(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    try:
        with pytest.raises(ValueError, match="Unsupported clarification_kind"):
            start_clarification(
                store=store,
                vision_brief="Setup",
                clarification_kind="unknown_kind",
            )
    finally:
        store.close()


def test_project_onboarding_profile_includes_extra_repositories(tmp_path):
    store = DevClarificationStore(db_path=tmp_path / "state.db")
    primary = tmp_path / "primary"
    secondary = tmp_path / "secondary"
    primary.mkdir()
    secondary.mkdir()
    try:
        session = start_clarification(
            store=store,
            vision_brief="",
            project_id="AlphaProject",
            clarification_kind="project_onboarding",
            project_context={"project_id": "AlphaProject", "project_name": "Alpha Project"},
        )
        answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id="onb_name",
            option_id="a",
            answer_text="Alpha Project",
        )
        answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id="onb_intent",
            option_id="b",
        )
        answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id="onb_vision",
            answer_text="Ship multi-repo onboarding.",
        )
        answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id="onb_repo",
            answer_text=str(primary),
        )
        answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id="onb_extra_repos",
            answer_text=f"{secondary}\n",
        )
        answer_clarification(
            store=store,
            clarification_id=session["clarification_id"],
            question_id="onb_constraints",
            option_id="a",
        )
        completed = complete_clarification(store=store, clarification_id=session["clarification_id"])
        repos = completed["clarified_brief"]["repositories"]
        assert len(repos) == 2
        assert any(repo["path"] == str(primary) for repo in repos)
        assert any(repo["path"] == str(secondary) for repo in repos)
        assert completed["clarified_brief"]["repos_deferred"] is False
    finally:
        store.close()
