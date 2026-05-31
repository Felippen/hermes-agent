import pytest

from gateway.dev_control.project_goal_slash import dispatch_project_goal_slash
from gateway.dev_control.project_goals import DevProjectGoalStore, create_project_goal


def test_project_slash_creates_vision(tmp_path):
    store = DevProjectGoalStore(tmp_path / "state.db")
    try:
        result = dispatch_project_goal_slash("vision", "North star", goal_store=store)
        assert result["type"] == "text"
        assert "Created vision" in result["content"]
        tree = store.tree("OrynWorkspace")
        assert tree["total"] == 1
    finally:
        store.close()


def test_project_slash_tree_and_list(tmp_path):
    store = DevProjectGoalStore(tmp_path / "state.db")
    try:
        vision = create_project_goal(store=store, kind="vision", title="Vision", project_id="OrynWorkspace")
        create_project_goal(
            store=store,
            kind="goal",
            title="Theme",
            project_id="OrynWorkspace",
            parent_goal_id=vision["goal_id"],
        )
        tree_result = dispatch_project_goal_slash("project", "tree", goal_store=store)
        assert tree_result["type"] == "text"
        assert "vision: Vision" in tree_result["content"]
        list_result = dispatch_project_goal_slash("project", "list goal", goal_store=store)
        assert list_result["type"] == "text"
        assert "Theme" in list_result["content"]
    finally:
        store.close()


def test_pgoal_requires_title(tmp_path):
    store = DevProjectGoalStore(tmp_path / "state.db")
    try:
        result = dispatch_project_goal_slash("pgoal", "", goal_store=store)
        assert result["type"] == "error"
    finally:
        store.close()
