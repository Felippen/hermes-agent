from gateway.dev_control.project_goal_digest import format_goal_tree_digest


def test_format_goal_tree_digest_renders_roots_and_plan_ids():
    digest = format_goal_tree_digest({
        "project_id": "OrynWorkspace",
        "total": 1,
        "roots": [{
            "kind": "vision",
            "title": "North star",
            "status": "active",
            "progress": 0.5,
            "goal_id": "g-vision",
            "children": [{
                "kind": "subgoal",
                "title": "Ship API",
                "status": "active",
                "progress": 0.25,
                "goal_id": "g-sub",
                "plan_artifact_id": "artifact-1",
                "payload": {"plan_id": "devplan-1"},
                "children": [],
            }],
        }],
    })

    assert "## Project goals" in digest
    assert "North star" in digest
    assert "Ship API" in digest
    assert "devplan-1" in digest
    assert "artifact-1" in digest
