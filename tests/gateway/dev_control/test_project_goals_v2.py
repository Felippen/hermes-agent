import os
from unittest.mock import patch

from gateway.dev_control.project_goals import (
    DevProjectGoalStore,
    create_project_goal,
    maybe_create_subgoal_for_approved_artifact,
)
from gateway.dev_control.project_goals_config import (
    project_goals_auto_subgoal_enabled,
    project_goals_tick_enabled,
)


def test_tick_enabled_from_env():
    with patch.dict(os.environ, {"HERMES_DEV_PROJECT_GOALS_TICK": "1"}, clear=False):
        with patch("hermes_cli.config.load_config", side_effect=Exception("no config")):
            assert project_goals_tick_enabled() is True


def test_tick_enabled_from_config():
    with patch.dict(os.environ, {}, clear=True):
        with patch(
            "hermes_cli.config.load_config",
            return_value={"dev": {"project_goals": {"tick_enabled": True}}},
        ):
            assert project_goals_tick_enabled() is True


def test_auto_subgoal_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_DEV_PROJECT_GOALS_AUTO_SUBGOAL", "1")
    store = DevProjectGoalStore(tmp_path / "state.db")
    try:
        vision = create_project_goal(
            store=store, kind="vision", title="Vision", project_id="OrynWorkspace",
        )
        theme = create_project_goal(
            store=store,
            kind="goal",
            title="Theme",
            project_id="OrynWorkspace",
            parent_goal_id=vision["goal_id"],
            status="active",
        )
        milestone = create_project_goal(
            store=store,
            kind="milestone",
            title="Ship slice",
            project_id="OrynWorkspace",
            parent_goal_id=theme["goal_id"],
            status="active",
        )
        artifact = {
            "plan_artifact_id": "artifact-1",
            "project_id": "OrynWorkspace",
            "title": "Approved plan",
            "markdown": "Plan body",
            "payload": {"milestone_goal_id": milestone["goal_id"], "acceptance_criteria": []},
        }
        first = maybe_create_subgoal_for_approved_artifact(store=store, artifact=artifact)
        second = maybe_create_subgoal_for_approved_artifact(store=store, artifact=artifact)
        assert first is not None
        assert second["goal_id"] == first["goal_id"]
        assert project_goals_auto_subgoal_enabled() is True
    finally:
        store.close()
