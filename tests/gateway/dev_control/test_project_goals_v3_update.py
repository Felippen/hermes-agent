import pytest

from gateway.dev_control.project_goals import (
    DevProjectGoalStore,
    create_project_goal,
    update_project_goal,
)


@pytest.fixture
def store(tmp_path):
    goal_store = DevProjectGoalStore(tmp_path / "state.db")
    yield goal_store
    goal_store.close()


def test_update_project_goal_title_and_status(store):
    vision = create_project_goal(
        store=store,
        kind="vision",
        title="Original",
        project_id="OrynWorkspace",
        status="active",
    )
    updated = update_project_goal(
        store=store,
        goal_id=vision["goal_id"],
        title="Renamed vision",
        status="blocked",
    )
    assert updated["title"] == "Renamed vision"
    assert updated["status"] == "blocked"


def test_update_project_goal_rejects_invalid_parent_kind(store):
    vision = create_project_goal(
        store=store,
        kind="vision",
        title="Vision",
        project_id="OrynWorkspace",
        status="active",
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
    subgoal = create_project_goal(
        store=store,
        kind="subgoal",
        title="Slice",
        project_id="OrynWorkspace",
        parent_goal_id=milestone["goal_id"],
        status="active",
    )
    with pytest.raises(ValueError, match="parent must be kind=milestone"):
        update_project_goal(
            store=store,
            goal_id=subgoal["goal_id"],
            parent_goal_id=theme["goal_id"],
        )
