import os
from unittest.mock import patch

import pytest

from gateway.dev_control.project_goals import (
    DevProjectGoalStore,
    create_project_goal,
    sync_subgoal_plan_id,
)
from gateway.dev_control.project_goal_eval import reevaluate_project_goal


@pytest.fixture
def store(tmp_path):
    goal_store = DevProjectGoalStore(tmp_path / "state.db")
    yield goal_store
    goal_store.close()


def _linked_subgoal(store, *, artifact_id="artifact-1", plan_id=None):
    vision = create_project_goal(
        store=store, kind="vision", title="Vision", project_id="OrynWorkspace", status="active",
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
        title="Milestone",
        project_id="OrynWorkspace",
        parent_goal_id=theme["goal_id"],
        status="active",
    )
    payload = {"plan_id": plan_id} if plan_id else {}
    return create_project_goal(
        store=store,
        kind="subgoal",
        title="Deliver slice",
        project_id="OrynWorkspace",
        parent_goal_id=milestone["goal_id"],
        status="active",
        plan_artifact_id=artifact_id,
        payload=payload,
    )


def test_sync_subgoal_plan_id_is_idempotent(store):
    subgoal = _linked_subgoal(store, artifact_id="artifact-sync")
    updated = sync_subgoal_plan_id(store, "devplan-abc", plan_artifact_id="artifact-sync")
    assert updated is not None
    assert updated["payload"]["plan_id"] == "devplan-abc"
    again = sync_subgoal_plan_id(store, "devplan-abc", plan_artifact_id="artifact-sync")
    assert again["goal_id"] == subgoal["goal_id"]
    assert again["payload"]["plan_id"] == "devplan-abc"


def test_reevaluate_blocks_on_failed_tasks(store, monkeypatch):
    monkeypatch.setenv("HERMES_DEV_PROJECT_GOALS_AUTO_BLOCK", "1")
    subgoal = _linked_subgoal(store, artifact_id="artifact-block")
    evidence = {"failed_task_count": 2, "verification": {"results": []}}
    with patch("gateway.dev_control.project_goal_eval.assemble_evidence", return_value=evidence):
        result = reevaluate_project_goal(store=store, goal_id=subgoal["goal_id"])
    assert result["verdict"] == "blocked"
    refreshed = store.get(subgoal["goal_id"])
    assert refreshed["status"] == "blocked"
    parent = store.get(refreshed["parent_goal_id"])
    assert parent["progress"] < 1.0


def test_auto_block_defaults_on_when_tick_enabled():
    with patch.dict(os.environ, {}, clear=True):
        with patch(
            "hermes_cli.config.load_config",
            return_value={"dev": {"project_goals": {"tick_enabled": True}}},
        ):
            from gateway.dev_control.project_goals_config import project_goals_auto_block_on_execution_failure

            assert project_goals_auto_block_on_execution_failure() is True
