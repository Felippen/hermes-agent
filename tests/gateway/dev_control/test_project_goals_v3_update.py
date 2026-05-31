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


def test_update_vision_goal_records_revision_history(store):
    vision = create_project_goal(
        store=store,
        kind="vision",
        title="Vision",
        project_id="OrynWorkspace",
        markdown="First draft",
        status="active",
    )
    updated = update_project_goal(
        store=store,
        goal_id=vision["goal_id"],
        markdown="Second draft",
        payload={"app_vision_version": 2},
    )
    assert updated["markdown"] == "Second draft"
    revisions = updated["payload"]["vision_revisions"]
    assert len(revisions) == 1
    assert revisions[0]["markdown"] == "First draft"
    assert revisions[0]["version"] == 1
    assert updated["payload"]["app_vision_version"] == 2


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
