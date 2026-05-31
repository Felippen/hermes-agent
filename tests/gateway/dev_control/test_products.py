import sqlite3

import pytest

from gateway.dev_control.acceptance_verification import DevVerificationStore
from gateway.dev_control.clarifications import DevClarificationStore, start_clarification
from gateway.dev_control.incidents import DevIncidentStore
from gateway.dev_control.products import (
    DevProductStore,
    build_product_action_surface,
    build_product_backlog,
    build_product_live_snapshot,
    build_product_portfolio,
    create_product,
    list_product_progression_loop,
    normalize_autonomy_level,
    normalize_flow_control_state,
    normalize_lifecycle_state,
    seed_products_from_project_ids,
    stable_product_id_for_project,
    tick_product_progression_loop,
    upsert_product,
)
from gateway.dev_control.project_goals import DevProjectGoalStore, create_project_goal
from gateway.dev_execution import DevExecutionStore
from tools.ao_bridge import AOSession


class FakeEventStore:
    def __init__(self, events):
        self.events = list(events)

    def list_events(self, **kwargs):
        ao_session_id = kwargs.get("ao_session_id")
        if ao_session_id:
            return [event for event in self.events if event.get("ao_session_id") == ao_session_id]
        return self.events


class FakeRuntimeRouter:
    def __init__(self):
        self.spawned = []

    def spawn(self, runtime: str, **kwargs):
        index = len(self.spawned) + 1
        session = AOSession(
            id=f"product-loop-test-{index}",
            project_id=kwargs.get("project_id"),
            status="running",
            branch=f"session/product-loop-test-{index}",
            workspace_path=f"/tmp/product-loop-test-{index}",
            tmux_name=f"tmux-product-loop-test-{index}",
            agent=kwargs.get("agent") or "codex",
            model=kwargs.get("model") or "gpt-5.5",
            reasoning_effort=kwargs.get("reasoning_effort"),
            open_command=f"tmux attach -t tmux-product-loop-test-{index}",
        )
        self.spawned.append({"runtime": runtime, "kwargs": kwargs, "session": session})
        return session


@pytest.fixture
def store(tmp_path):
    product_store = DevProductStore(tmp_path / "state.db")
    yield product_store
    product_store.close()


def test_create_product_persists_across_restart(tmp_path):
    db_path = tmp_path / "state.db"
    store_a = DevProductStore(db_path)
    created = create_product(
        store=store_a,
        project_id="OrynWorkspace",
        name="Oryn Workspace",
        lifecycle_state="active",
        repository_bindings=[{"path": "/tmp/oryn", "label": "Oryn"}],
    )
    store_a.close()

    store_b = DevProductStore(db_path)
    loaded = store_b.get(created["product_id"])
    store_b.close()

    assert loaded["product_id"] == created["product_id"]
    assert loaded["project_id"] == "OrynWorkspace"
    assert loaded["name"] == "Oryn Workspace"
    assert loaded["lifecycle_state"] == "active"
    assert loaded["repository_bindings"][0]["path"] == "/tmp/oryn"


def test_unknown_lifecycle_round_trips_as_unknown(store):
    created = create_product(
        store=store,
        project_id="Ovyon",
        name="Ovyon",
        lifecycle_state="surprising-new-state",
    )

    assert normalize_lifecycle_state("surprising-new-state") == "unknown"
    assert created["lifecycle_state"] == "unknown"
    assert store.get(created["product_id"])["lifecycle_state"] == "unknown"


def test_flow_control_persists_across_restart(tmp_path):
    db_path = tmp_path / "state.db"
    store_a = DevProductStore(db_path)
    created = create_product(store=store_a, project_id="OrynWorkspace", name="Oryn")
    updated = store_a.update_flow_control(
        created["product_id"],
        state="paused",
        reason="Reframe scope",
        requested_by="Felipe",
        now=123.0,
    )
    store_a.close()

    store_b = DevProductStore(db_path)
    loaded = store_b.get(created["product_id"])
    store_b.close()

    assert updated["flow_control"]["state"] == "paused"
    assert loaded["flow_control"]["state"] == "paused"
    assert loaded["flow_control"]["reason"] == "Reframe scope"
    assert loaded["flow_control"]["requested_by"] == "Felipe"
    assert loaded["flow_control"]["autonomy_level"] == "supervised"
    assert loaded["payload"]["flow_control_history"][-1]["state"] == "paused"


def test_product_flow_control_records_autonomy_level(store):
    created = create_product(store=store, project_id="OrynWorkspace", name="Oryn")

    updated = store.update_flow_control(
        created["product_id"],
        state="normal",
        autonomy_level="bounded",
        requested_by="Felipe",
        now=123.0,
    )

    assert normalize_autonomy_level("bounded") == "bounded"
    assert normalize_autonomy_level("ship-it") == "unknown"
    assert updated["flow_control"]["state"] == "normal"
    assert updated["flow_control"]["autonomy_level"] == "bounded"
    assert updated["payload"]["flow_control_history"][-1]["autonomy_level"] == "bounded"


def test_portfolio_flow_control_persists_across_restart(tmp_path):
    db_path = tmp_path / "state.db"
    store_a = DevProductStore(db_path)
    default = store_a.get_portfolio_flow_control()
    updated = store_a.update_portfolio_flow_control(
        state="paused",
        autonomy_level="manual",
        reason="Freeze portfolio",
        requested_by="Felipe",
        now=456.0,
    )
    store_a.close()

    store_b = DevProductStore(db_path)
    loaded = store_b.get_portfolio_flow_control()
    store_b.close()

    assert default["state"] == "normal"
    assert default["autonomy_level"] == "supervised"
    assert updated["object"] == "hermes.dev_portfolio_flow_control"
    assert loaded["state"] == "paused"
    assert loaded["autonomy_level"] == "manual"
    assert loaded["reason"] == "Freeze portfolio"
    assert loaded["requested_by"] == "Felipe"


def test_two_products_keep_distinct_flow_control_state(store):
    first = create_product(store=store, project_id="OrynWorkspace", name="Oryn")
    second = create_product(store=store, project_id="Ovyon", name="Ovyon")

    store.update_flow_control(first["product_id"], state="hold_new_work", requested_by="Felipe")

    assert store.get(first["product_id"])["flow_control"]["state"] == "hold_new_work"
    assert store.get(second["product_id"])["flow_control"]["state"] == "normal"


def test_unknown_flow_control_is_safe(store):
    created = create_product(
        store=store,
        project_id="OrynWorkspace",
        name="Oryn",
        payload={"flow_control": {"state": "ship-it"}},
    )

    assert normalize_flow_control_state("ship-it") == "unknown"
    assert created["flow_control"]["state"] == "unknown"
    assert store.get(created["product_id"])["flow_control"]["state"] == "unknown"


def test_two_products_keep_distinct_state(store):
    first = create_product(
        store=store,
        project_id="OrynWorkspace",
        name="Oryn Workspace",
        lifecycle_state="active",
    )
    second = create_product(
        store=store,
        project_id="Ovyon",
        name="Ovyon",
        lifecycle_state="paused",
    )

    products = store.list()
    by_project = {item["project_id"]: item for item in products}
    assert by_project["OrynWorkspace"]["product_id"] == first["product_id"]
    assert by_project["OrynWorkspace"]["lifecycle_state"] == "active"
    assert by_project["Ovyon"]["product_id"] == second["product_id"]
    assert by_project["Ovyon"]["lifecycle_state"] == "paused"


def test_active_project_id_is_unique(store):
    create_product(store=store, project_id="OrynWorkspace", name="Oryn")

    with pytest.raises(sqlite3.IntegrityError):
        create_product(store=store, project_id="OrynWorkspace", name="Duplicate")


def test_archived_product_allows_replacement_for_project_id(store):
    first = create_product(store=store, project_id="OrynWorkspace", name="Oryn")
    archived = store.archive(first["product_id"])
    replacement = create_product(store=store, project_id="OrynWorkspace", name="Oryn 2")

    assert archived["lifecycle_state"] == "archived"
    assert replacement["product_id"] != first["product_id"]
    assert store.get_by_project_id("OrynWorkspace")["product_id"] == replacement["product_id"]


def test_upsert_is_duplicate_safe_and_preserves_product_id(store):
    first = upsert_product(
        store=store,
        project_id="OrynWorkspace",
        name="Oryn",
        lifecycle_state="planned",
    )
    second = upsert_product(
        store=store,
        project_id="OrynWorkspace",
        name="Oryn Workspace",
        lifecycle_state="active",
    )

    assert second["product_id"] == first["product_id"]
    assert second["name"] == "Oryn Workspace"
    assert second["lifecycle_state"] == "active"
    assert len(store.list()) == 1


def test_seed_products_from_project_ids_is_idempotent(store):
    seeded_once = seed_products_from_project_ids(
        store=store,
        project_ids=["OrynWorkspace", "Ovyon", "OrynWorkspace"],
        workspace_projects=[
            {
                "id": "workspace-record-1",
                "hermes_project_id": "OrynWorkspace",
                "name": "Oryn Workspace",
                "vision": "Operator console",
                "coordinator_profile": "dev",
                "repos": [{"path": "/repo/oryn"}],
            }
        ],
    )
    seeded_twice = seed_products_from_project_ids(
        store=store,
        project_ids=["OrynWorkspace", "Ovyon"],
    )

    assert len(seeded_once) == 2
    assert len(seeded_twice) == 2
    assert len(store.list()) == 2
    assert store.get_by_project_id("OrynWorkspace")["name"] == "Oryn Workspace"
    assert store.get_by_project_id("OrynWorkspace")["primary_repo"] == "/repo/oryn"
    assert store.get_by_project_id("Ovyon")["product_id"] == stable_product_id_for_project("Ovyon")


def test_empty_store_is_safe_rollback_state(store):
    assert store.list() == []
    assert store.get_by_project_id("OrynWorkspace") is None


def test_backlog_derives_plan_task_states_from_execution_truth(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        product = create_product(
            store=product_store,
            project_id="OrynWorkspace",
            name="Oryn Workspace",
        )
        plan = execution_store.create_plan(
            title="Product slice",
            vision_brief="Move product forward",
            tasks=[
                {
                    "task_id": "task-running",
                    "goal": "Implement API",
                    "prompt": "Implement API",
                    "project_id": "OrynWorkspace",
                },
                {
                    "task_id": "task-planned",
                    "goal": "Add UI",
                    "prompt": "Add UI",
                    "project_id": "OrynWorkspace",
                },
            ],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="task-running",
            ao_session_id="ao-running",
            status="launched",
        )

        backlog = build_product_backlog(product=product, execution_store=execution_store)

        states = {item["source"]["task_id"]: item["state"] for item in backlog["items"]}
        assert states == {"task-running": "in_flight", "task-planned": "planned"}
        assert backlog["counts"]["total"] == 2
        assert backlog["next_item_id"] == f"backlog-task-{plan['plan_id']}-task-planned"
    finally:
        execution_store.close()
        product_store.close()


def test_backlog_links_worker_verification_launch_and_incident_evidence(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    verification_store = DevVerificationStore(db_path)
    incident_store = DevIncidentStore(db_path)
    try:
        product = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn")
        plan = execution_store.create_plan(
            title="Incident-linked work",
            vision_brief=None,
            tasks=[{
                "task_id": "task-1",
                "goal": "Ship guarded slice",
                "prompt": "Ship guarded slice",
                "project_id": "OrynWorkspace",
            }],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="task-1",
            ao_session_id="ao-1",
            status="launched",
        )
        draft = execution_store.create_draft_review(
            plan_id=plan["plan_id"],
            plan_artifact_id="artifact-1",
            build_id="build-1",
        )
        launch = execution_store.append_launch_record(
            plan_id=plan["plan_id"],
            draft_review=draft,
            requested_task_ids=["task-1"],
            launched=[{"task_id": "task-1", "ao_session_id": "ao-1"}],
            failures=[],
        )
        verification = verification_store.create_run(
            plan_id=plan["plan_id"],
            task_id="task-1",
            target_type="task",
            status="completed",
            results=[{"status": "passed"}],
            executable_commands=[],
            verified_against={"kind": "task"},
        )
        incident = incident_store.create_incident({
            "object": "hermes.dev_incident",
            "incident_id": "devinc-test",
            "detected_at": 123.0,
            "severity": "high",
            "status": "detected",
            "title": "Task regression",
            "correlated_release": {"plan_id": plan["plan_id"], "task_id": "task-1"},
            "evidence_refs": [{"plan_id": plan["plan_id"], "task_id": "task-1"}],
            "clusters": [],
            "recommendation": {},
            "postmortem": {},
            "proposal_id": None,
            "warnings": [],
            "created_at": 123.0,
            "updated_at": 123.0,
            "acknowledged_at": None,
            "resolved_at": None,
        })
        event_store = FakeEventStore([
            {
                "event_id": 7,
                "event": "subagent.progress",
                "ao_session_id": "ao-1",
                "status": "running",
                "created_at": 124.0,
            }
        ])

        backlog = build_product_backlog(
            product=product,
            execution_store=execution_store,
            verification_store=verification_store,
            incident_store=incident_store,
            event_store=event_store,
        )

        item = backlog["items"][0]
        evidence_kinds = {link["kind"] for link in item["evidence_links"]}
        assert item["state"] == "blocked"
        assert item["blocking_reason"] == "Unresolved incident is linked to this work."
        assert {"worker_session", "dev_execution_launch", "dev_verification_run", "dev_incident", "subagent_event"} <= evidence_kinds
        assert any(link.get("launch_id") == launch["launch_id"] for link in item["evidence_links"])
        assert any(link.get("verification_run_id") == verification["verification_run_id"] for link in item["evidence_links"])
        assert any(link.get("incident_id") == incident["incident_id"] for link in item["evidence_links"])
    finally:
        incident_store.close()
        verification_store.close()
        execution_store.close()
        product_store.close()


def test_backlog_groups_tasks_and_goal_only_items_by_milestone(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    goal_store = DevProjectGoalStore(db_path)
    try:
        product = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn")
        vision = create_project_goal(store=goal_store, kind="vision", title="Vision", project_id="OrynWorkspace")
        goal = create_project_goal(
            store=goal_store,
            kind="goal",
            title="Theme",
            project_id="OrynWorkspace",
            parent_goal_id=vision["goal_id"],
        )
        milestone = create_project_goal(
            store=goal_store,
            kind="milestone",
            title="Ship M1",
            project_id="OrynWorkspace",
            parent_goal_id=goal["goal_id"],
            ordering=10,
        )
        linked_subgoal = create_project_goal(
            store=goal_store,
            kind="subgoal",
            title="Backlog from task",
            project_id="OrynWorkspace",
            parent_goal_id=milestone["goal_id"],
        )
        goal_only_subgoal = create_project_goal(
            store=goal_store,
            kind="subgoal",
            title="Goal-only backlog",
            project_id="OrynWorkspace",
            parent_goal_id=milestone["goal_id"],
        )
        execution_store.create_plan(
            title="Milestoned work",
            vision_brief=None,
            tasks=[{
                "task_id": "task-linked",
                "goal": "Task linked to subgoal",
                "prompt": "Task linked to subgoal",
                "project_id": "OrynWorkspace",
                "linked_goal_id": linked_subgoal["goal_id"],
            }],
        )

        backlog = build_product_backlog(
            product=product,
            execution_store=execution_store,
            goal_store=goal_store,
        )

        group = next(group for group in backlog["milestone_groups"] if group["milestone_id"] == milestone["goal_id"])
        assert group["title"] == "Ship M1"
        assert group["counts"]["total"] == 2
        assert any(item["source"].get("task_id") == "task-linked" for item in backlog["items"])
        assert any(item["source"].get("goal_id") == goal_only_subgoal["goal_id"] for item in backlog["items"])
    finally:
        goal_store.close()
        execution_store.close()
        product_store.close()


def test_manual_work_item_status_does_not_override_execution_truth(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        product = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn")
        plan = execution_store.create_plan(
            title="Manual conflict",
            vision_brief=None,
            tasks=[{
                "task_id": "task-planned",
                "goal": "Still planned",
                "prompt": "Still planned",
                "project_id": "OrynWorkspace",
            }],
        )

        backlog = build_product_backlog(
            product=product,
            execution_store=execution_store,
            manual_work_items=[{
                "id": "manual-1",
                "title": "Operator item",
                "status": "done",
                "linked_dev_plan_id": plan["plan_id"],
                "linked_task_id": "task-planned",
            }],
        )

        item = backlog["items"][0]
        assert item["state"] == "planned"
        assert item["manual_context"]["status"] == "done"
    finally:
        execution_store.close()
        product_store.close()


def test_backlog_keeps_two_products_isolated(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        oryn = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn")
        ovyon = create_product(store=product_store, project_id="Ovyon", name="Ovyon")
        execution_store.create_plan(
            title="Oryn work",
            vision_brief=None,
            tasks=[{
                "task_id": "oryn-task",
                "goal": "Oryn task",
                "prompt": "Oryn task",
                "project_id": "OrynWorkspace",
            }],
        )
        execution_store.create_plan(
            title="Ovyon work",
            vision_brief=None,
            tasks=[{
                "task_id": "ovyon-task",
                "goal": "Ovyon task",
                "prompt": "Ovyon task",
                "project_id": "Ovyon",
            }],
        )

        oryn_backlog = build_product_backlog(product=oryn, execution_store=execution_store)
        ovyon_backlog = build_product_backlog(product=ovyon, execution_store=execution_store)

        assert [item["source"]["task_id"] for item in oryn_backlog["items"]] == ["oryn-task"]
        assert [item["source"]["task_id"] for item in ovyon_backlog["items"]] == ["ovyon-task"]
    finally:
        execution_store.close()
        product_store.close()


def test_portfolio_empty_store_is_unknown_safe(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        portfolio = build_product_portfolio(store=product_store, execution_store=execution_store)

        assert portfolio["object"] == "hermes.dev_product_portfolio"
        assert portfolio["flow_control"]["state"] == "normal"
        assert portfolio["flow_control"]["autonomy_level"] == "supervised"
        assert portfolio["total"] == 0
        assert portfolio["counts"] == {
            "total": 0,
            "needs_attention": 0,
            "active": 0,
            "planned": 0,
            "complete": 0,
            "unknown": 0,
        }
        assert portfolio["items"] == []
    finally:
        execution_store.close()
        product_store.close()


def test_product_action_surface_empty_store_is_read_only(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        surface = build_product_action_surface(store=product_store, execution_store=execution_store)

        assert surface["object"] == "hermes.dev_product_actions"
        assert surface["total"] == 0
        assert surface["counts"]["total"] == 0
        assert surface["data"] == []
        assert execution_store.list_plans() == []
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_empty_store_is_safe(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        listed = list_product_progression_loop(store=product_store)
        tick = tick_product_progression_loop(store=product_store, execution_store=execution_store, now=123.0)

        assert listed["object"] == "hermes.dev_product_progression_loop"
        assert listed["total"] == 0
        assert listed["data"] == []
        assert tick["object"] == "hermes.dev_product_progression_loop_tick"
        assert tick["evaluated_count"] == 0
        assert tick["data"] == []
        assert execution_store.list_plans() == []
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_persists_latest_state_and_isolates_target_product(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        oryn = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn", lifecycle_state="active")
        ovyon = create_product(store=product_store, project_id="Ovyon", name="Ovyon", lifecycle_state="active")
        execution_store.create_plan(
            title="Oryn planned work",
            vision_brief=None,
            tasks=[{
                "task_id": "oryn-planned",
                "goal": "Advance Product",
                "prompt": "Advance Product",
                "project_id": "OrynWorkspace",
            }],
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            product_id=oryn["product_id"],
            now=200.0,
        )
        latest = list_product_progression_loop(store=product_store)

        assert tick["evaluated_count"] == 1
        assert tick["data"][0]["product_id"] == oryn["product_id"]
        assert tick["data"][0]["status"] == "advanced"
        assert tick["data"][0]["backlog_counts"]["planned"] == 1
        assert latest["total"] == 1
        assert latest["data"][0]["product_id"] == oryn["product_id"]
        assert product_store.list_progression_iterations(product_id=ovyon["product_id"]) == []
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_honors_flow_control_states(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        paused = create_product(store=product_store, project_id="Paused", name="Paused", lifecycle_state="active")
        held = create_product(store=product_store, project_id="Held", name="Held", lifecycle_state="active")
        direction = create_product(store=product_store, project_id="Direction", name="Direction", lifecycle_state="active")
        product_store.update_flow_control(paused["product_id"], state="paused", reason="Pause", now=100.0)
        product_store.update_flow_control(held["product_id"], state="hold_new_work", reason="Hold", now=101.0)
        product_store.update_flow_control(direction["product_id"], state="needs_direction", reason="Choose", now=102.0)

        tick = tick_product_progression_loop(store=product_store, execution_store=execution_store, now=201.0)
        by_product = {item["product_id"]: item for item in tick["data"]}

        assert tick["counts"]["held_by_flow_control"] == 3
        assert by_product[paused["product_id"]]["status"] == "held_by_flow_control"
        assert by_product[held["product_id"]]["reason"] == "Hold"
        assert by_product[direction["product_id"]]["selected_action_kind"] == "direction_needed"
        assert execution_store.list_plans() == []
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_manual_product_autonomy_blocks_advisory_advance(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        product = create_product(store=product_store, project_id="ManualProduct", name="Manual", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="manual", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        execution_store.create_plan(
            title="Manual Product planned work",
            vision_brief=None,
            tasks=[{
                "task_id": "manual-planned",
                "goal": "Manual planned work",
                "prompt": "Manual planned work",
                "project_id": "ManualProduct",
            }],
        )

        tick = tick_product_progression_loop(store=product_store, execution_store=execution_store, now=201.0)
        item = tick["data"][0]

        assert item["status"] == "waiting_for_human"
        assert item["reason"] == "Manual autonomy requires Felipe to initiate Product progression."
        assert item["autonomy_policy"] == {
            "product_autonomy_level": "manual",
            "portfolio_autonomy_level": "bounded",
            "effective_autonomy_level": "manual",
            "decision": "manual_waiting_for_human",
            "reason": "Manual autonomy requires Felipe to initiate Product progression.",
        }
        assert item["backlog_counts"]["planned"] == 1
        assert len(execution_store.list_plans(project_id="ManualProduct")) == 1
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_manual_portfolio_autonomy_blocks_bounded_product(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        product = create_product(store=product_store, project_id="BoundedProduct", name="Bounded", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="manual", now=101.0)
        execution_store.create_plan(
            title="Bounded Product planned work",
            vision_brief=None,
            tasks=[{
                "task_id": "bounded-planned",
                "goal": "Bounded planned work",
                "prompt": "Bounded planned work",
                "project_id": "BoundedProduct",
            }],
        )

        tick = tick_product_progression_loop(store=product_store, execution_store=execution_store, now=201.0)
        item = tick["data"][0]

        assert item["status"] == "waiting_for_human"
        assert item["autonomy_policy"]["product_autonomy_level"] == "bounded"
        assert item["autonomy_policy"]["portfolio_autonomy_level"] == "manual"
        assert item["autonomy_policy"]["effective_autonomy_level"] == "manual"
        assert item["autonomy_policy"]["decision"] == "manual_waiting_for_human"
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_bounded_autonomy_launches_one_planned_task(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="BoundedLaunch", name="Bounded Launch", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Bounded launch work",
            vision_brief=None,
            tasks=[{
                "task_id": "bounded-launch-task",
                "goal": "Launch bounded worker",
                "prompt": "Launch bounded worker",
                "project_id": "BoundedLaunch",
            }],
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]
        launched_plan = execution_store.get_plan(plan["plan_id"])

        assert item["status"] == "advanced"
        assert item["reason"] == "Bounded autonomy launched planned Product execution work."
        assert item["autonomy_policy"]["decision"] == "bounded_launch_applied"
        assert item["transition"]["action"] == "launch_execution_task"
        assert item["transition"]["status"] == "applied"
        assert item["transition"]["plan_id"] == plan["plan_id"]
        assert item["transition"]["task_id"] == "bounded-launch-task"
        assert item["transition"]["launch"]["launched_count"] == 1
        assert item["transition"]["launch"]["launched_task_ids"] == ["bounded-launch-task"]
        assert launched_plan["tasks"][0]["status"] == "launched"
        assert launched_plan["tasks"][0]["ao_session_id"]
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_bounded_autonomy_launches_verification_for_completed_task(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    verification_store = DevVerificationStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="BoundedVerify", name="Bounded Verify", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Completed work",
            vision_brief=None,
            tasks=[{
                "task_id": "completed-task",
                "goal": "Complete then verify",
                "prompt": "Complete then verify",
                "project_id": "BoundedVerify",
                "acceptance_criteria": [
                    "Workspace tests pass (verify via command: make test; machine-checkable)",
                ],
            }],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="completed-task",
            ao_session_id="ao-completed",
            status="completed",
        )
        event_store = FakeEventStore([{
            "event": "subagent.complete",
            "ao_session_id": "ao-completed",
            "launch_plan_id": plan["plan_id"],
            "launch_task_id": "completed-task",
            "status": "completed",
            "summary": "Completed bounded verification setup.",
            "created_at": 200.0,
        }])

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            verification_store=verification_store,
            event_store=event_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]
        runs = verification_store.list_runs(plan_id=plan["plan_id"], task_id="completed-task")

        assert item["status"] == "advanced"
        assert item["reason"] == "Bounded autonomy launched acceptance verification."
        assert item["autonomy_policy"]["decision"] == "bounded_verification_applied"
        assert item["transition"]["action"] == "launch_acceptance_verification"
        assert item["transition"]["status"] == "applied"
        assert item["transition"]["plan_id"] == plan["plan_id"]
        assert item["transition"]["task_id"] == "completed-task"
        assert item["transition"]["verification"]["verification_run_id"] == runs[0]["verification_run_id"]
        assert item["transition"]["verification"]["status"] == "launched"
        assert runs[0]["status"] == "launched"
        assert runs[0]["verification_session_id"] == "product-loop-test-1"
        assert len(bridge.spawned) == 1
    finally:
        verification_store.close()
        execution_store.close()
        product_store.close()


def test_product_progression_loop_existing_verification_prevents_duplicate_launch(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    verification_store = DevVerificationStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="AlreadyVerified", name="Already Verified", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Already verified work",
            vision_brief=None,
            tasks=[{
                "task_id": "already-verified",
                "goal": "Do not duplicate verifier",
                "prompt": "Do not duplicate verifier",
                "project_id": "AlreadyVerified",
                "acceptance_criteria": [
                    "Tests pass (verify via command: make test; machine-checkable)",
                ],
            }],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="already-verified",
            ao_session_id="ao-completed",
            status="completed",
        )
        existing = verification_store.create_run(
            plan_id=plan["plan_id"],
            task_id="already-verified",
            target_type="task",
            status="completed",
            results=[],
            executable_commands=[],
            verified_against={},
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            verification_store=verification_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]
        runs = verification_store.list_runs(plan_id=plan["plan_id"], task_id="already-verified")

        assert item["status"] == "advanced"
        assert "transition" not in item
        assert [run["verification_run_id"] for run in runs] == [existing["verification_run_id"]]
        assert bridge.spawned == []
    finally:
        verification_store.close()
        execution_store.close()
        product_store.close()


def test_product_progression_loop_supervised_autonomy_does_not_launch_planned_task(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="SupervisedLaunch", name="Supervised Launch", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="supervised", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Supervised planned work",
            vision_brief=None,
            tasks=[{
                "task_id": "supervised-planned",
                "goal": "Stay advisory",
                "prompt": "Stay advisory",
                "project_id": "SupervisedLaunch",
            }],
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]
        latest_plan = execution_store.get_plan(plan["plan_id"])

        assert item["status"] == "advanced"
        assert item["autonomy_policy"]["effective_autonomy_level"] == "supervised"
        assert "transition" not in item
        assert bridge.spawned == []
        assert latest_plan["tasks"][0]["status"] == "planned"
        assert latest_plan["tasks"][0]["ao_session_id"] is None
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_supervised_autonomy_does_not_launch_verification(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    verification_store = DevVerificationStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="SupervisedVerify", name="Supervised Verify", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="supervised", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Supervised complete work",
            vision_brief=None,
            tasks=[{
                "task_id": "supervised-complete",
                "goal": "Stay advisory after completion",
                "prompt": "Stay advisory after completion",
                "project_id": "SupervisedVerify",
                "acceptance_criteria": [
                    "Tests pass (verify via command: make test; machine-checkable)",
                ],
            }],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="supervised-complete",
            ao_session_id="ao-completed",
            status="completed",
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            verification_store=verification_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]

        assert item["status"] == "advanced"
        assert item["autonomy_policy"]["effective_autonomy_level"] == "supervised"
        assert item["autonomy_policy"]["decision"] == "advisory_verification_advance"
        assert "transition" not in item
        assert verification_store.list_runs(plan_id=plan["plan_id"], task_id="supervised-complete") == []
        assert bridge.spawned == []
    finally:
        verification_store.close()
        execution_store.close()
        product_store.close()


def test_product_progression_loop_bounded_autonomy_still_waits_on_high_priority_actions(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    clarification_store = DevClarificationStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="BoundedHuman", name="Bounded Human", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        start_clarification(
            store=clarification_store,
            project_id="BoundedHuman",
            vision_brief="Choose Product direction",
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            clarification_store=clarification_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]

        assert item["status"] == "waiting_for_human"
        assert item["selected_action_kind"] == "clarification"
        assert item["autonomy_policy"]["effective_autonomy_level"] == "bounded"
        assert item["autonomy_policy"]["decision"] == "human_action_required"
        assert "transition" not in item
        assert bridge.spawned == []
        assert execution_store.list_plans(project_id="BoundedHuman") == []
    finally:
        clarification_store.close()
        execution_store.close()
        product_store.close()


def test_product_progression_loop_human_gate_blocks_bounded_verification(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    verification_store = DevVerificationStore(db_path)
    clarification_store = DevClarificationStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="VerifyHumanGate", name="Verify Human Gate", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Complete gated work",
            vision_brief=None,
            tasks=[{
                "task_id": "complete-gated",
                "goal": "Wait for human before verification",
                "prompt": "Wait for human before verification",
                "project_id": "VerifyHumanGate",
                "acceptance_criteria": [
                    "Tests pass (verify via command: make test; machine-checkable)",
                ],
            }],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="complete-gated",
            ao_session_id="ao-completed",
            status="completed",
        )
        start_clarification(
            store=clarification_store,
            project_id="VerifyHumanGate",
            vision_brief="Choose verification direction",
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            verification_store=verification_store,
            clarification_store=clarification_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]

        assert item["status"] == "waiting_for_human"
        assert item["selected_action_kind"] == "clarification"
        assert item["autonomy_policy"]["decision"] == "human_action_required"
        assert "transition" not in item
        assert verification_store.list_runs(plan_id=plan["plan_id"], task_id="complete-gated") == []
        assert bridge.spawned == []
    finally:
        clarification_store.close()
        verification_store.close()
        execution_store.close()
        product_store.close()


def test_product_progression_loop_records_skipped_verification_transition(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    verification_store = DevVerificationStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="SkippedVerify", name="Skipped Verify", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Manual-only criteria",
            vision_brief=None,
            tasks=[{
                "task_id": "manual-only",
                "goal": "Complete with manual criteria",
                "prompt": "Complete with manual criteria",
                "project_id": "SkippedVerify",
                "acceptance_criteria": [{
                    "statement": "Review the final behavior",
                    "verification_method": "manual",
                    "verification_detail": "Review manually.",
                    "machine_checkable": False,
                }],
            }],
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="manual-only",
            ao_session_id="ao-completed",
            status="completed",
        )
        event_store = FakeEventStore([{
            "event": "subagent.complete",
            "ao_session_id": "ao-completed",
            "launch_plan_id": plan["plan_id"],
            "launch_task_id": "manual-only",
            "status": "completed",
            "summary": "Completed manual-only work.",
            "created_at": 200.0,
        }])

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            verification_store=verification_store,
            event_store=event_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]
        runs = verification_store.list_runs(plan_id=plan["plan_id"], task_id="manual-only")

        assert item["status"] == "advanced"
        assert item["reason"] == "Bounded autonomy recorded skipped acceptance verification."
        assert item["autonomy_policy"]["decision"] == "bounded_verification_skipped"
        assert item["transition"]["action"] == "launch_acceptance_verification"
        assert item["transition"]["status"] == "skipped"
        assert item["transition"]["verification"]["verification_run_id"] == runs[0]["verification_run_id"]
        assert item["transition"]["verification"]["status"] == "skipped"
        assert runs[0]["status"] == "skipped"
        assert bridge.spawned == []
    finally:
        verification_store.close()
        execution_store.close()
        product_store.close()


def test_product_progression_loop_bounded_launch_preserves_draft_gate(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    bridge = FakeRuntimeRouter()
    try:
        product = create_product(store=product_store, project_id="DraftGate", name="Draft Gate", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="normal", autonomy_level="bounded", now=100.0)
        product_store.update_portfolio_flow_control(state="normal", autonomy_level="bounded", now=101.0)
        plan = execution_store.create_plan(
            title="Draft gated work",
            vision_brief=None,
            tasks=[{
                "task_id": "draft-gated",
                "goal": "Wait for draft approval",
                "prompt": "Wait for draft approval",
                "project_id": "DraftGate",
            }],
        )
        execution_store.create_draft_review(
            plan_id=plan["plan_id"],
            plan_artifact_id="artifact-draft",
            build_id="build-draft",
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            bridge=bridge,
            now=201.0,
        )
        item = tick["data"][0]
        latest_plan = execution_store.get_plan(plan["plan_id"])

        assert item["status"] == "waiting_for_human"
        assert item["selected_action_kind"] == "draft_review"
        assert item["autonomy_policy"]["decision"] == "human_action_required"
        assert "transition" not in item
        assert bridge.spawned == []
        assert latest_plan["tasks"][0]["status"] == "planned"
        assert latest_plan["tasks"][0]["ao_session_id"] is None
    finally:
        execution_store.close()
        product_store.close()


def test_product_progression_loop_classifies_human_blocked_active_and_idle(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    clarification_store = DevClarificationStore(db_path)
    try:
        human = create_product(store=product_store, project_id="Human", name="Human", lifecycle_state="active")
        blocked = create_product(store=product_store, project_id="Blocked", name="Blocked", lifecycle_state="active")
        active = create_product(store=product_store, project_id="Active", name="Active", lifecycle_state="active")
        idle = create_product(store=product_store, project_id="Idle", name="Idle", lifecycle_state="complete")
        start_clarification(
            store=clarification_store,
            project_id="Human",
            vision_brief="Needs Product input",
        )
        blocked_plan = execution_store.create_plan(
            title="Blocked work",
            vision_brief=None,
            tasks=[{
                "task_id": "blocked-task",
                "goal": "Blocked",
                "prompt": "Blocked",
                "project_id": "Blocked",
            }],
        )
        execution_store.update_task_launch(
            plan_id=blocked_plan["plan_id"],
            task_id="blocked-task",
            ao_session_id="ao-blocked",
            status="failed",
        )
        execution_store.create_plan(
            title="Active work",
            vision_brief=None,
            tasks=[{
                "task_id": "active-task",
                "goal": "Active",
                "prompt": "Active",
                "project_id": "Active",
            }],
        )

        tick = tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            clarification_store=clarification_store,
            now=300.0,
        )
        by_product = {item["product_id"]: item for item in tick["data"]}

        assert by_product[human["product_id"]]["status"] == "waiting_for_human"
        assert by_product[human["product_id"]]["selected_action_kind"] == "clarification"
        assert by_product[blocked["product_id"]]["status"] == "blocked"
        assert by_product[blocked["product_id"]]["source_refs"][-1]["state"] == "failed"
        assert by_product[active["product_id"]]["status"] == "advanced"
        assert by_product[idle["product_id"]]["status"] == "idle"
        assert tick["counts"]["waiting_for_human"] == 1
        assert tick["counts"]["blocked"] == 1
        assert tick["counts"]["advanced"] == 1
        assert tick["counts"]["idle"] == 1
    finally:
        clarification_store.close()
        execution_store.close()
        product_store.close()


def test_portfolio_keeps_two_products_isolated_and_attention_first(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        oryn = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn", lifecycle_state="active")
        ovyon = create_product(store=product_store, project_id="Ovyon", name="Ovyon", lifecycle_state="active")
        oryn_plan = execution_store.create_plan(
            title="Oryn blocked work",
            vision_brief=None,
            tasks=[{
                "task_id": "oryn-blocked",
                "goal": "Needs intervention",
                "prompt": "Needs intervention",
                "project_id": "OrynWorkspace",
            }],
        )
        execution_store.update_task_launch(
            plan_id=oryn_plan["plan_id"],
            task_id="oryn-blocked",
            ao_session_id="ao-oryn",
            status="failed",
        )
        ovyon_plan = execution_store.create_plan(
            title="Ovyon running work",
            vision_brief=None,
            tasks=[{
                "task_id": "ovyon-running",
                "goal": "Keep running",
                "prompt": "Keep running",
                "project_id": "Ovyon",
            }],
        )
        execution_store.update_task_launch(
            plan_id=ovyon_plan["plan_id"],
            task_id="ovyon-running",
            ao_session_id="ao-ovyon",
            status="launched",
        )

        portfolio = build_product_portfolio(store=product_store, execution_store=execution_store)

        assert [item["product_id"] for item in portfolio["items"]] == [oryn["product_id"], ovyon["product_id"]]
        assert portfolio["counts"]["needs_attention"] == 1
        assert portfolio["counts"]["active"] == 1
        by_project = {item["project_id"]: item for item in portfolio["items"]}
        assert by_project["OrynWorkspace"]["attention_state"] == "needs_attention"
        assert by_project["OrynWorkspace"]["backlog_counts"]["failed"] == 1
        assert by_project["OrynWorkspace"]["next_item"]["source"]["task_id"] == "oryn-blocked"
        assert by_project["Ovyon"]["attention_state"] == "active"
        assert by_project["Ovyon"]["backlog_counts"]["in_flight"] == 1
        assert by_project["Ovyon"]["next_item"]["source"]["task_id"] == "ovyon-running"
    finally:
        execution_store.close()
        product_store.close()


def test_portfolio_projects_flow_control_attention_and_order(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        oryn = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn", lifecycle_state="active")
        ovyon = create_product(store=product_store, project_id="Ovyon", name="Ovyon", lifecycle_state="active")
        product_store.update_flow_control(ovyon["product_id"], state="needs_direction", reason="Pick next milestone", now=200.0)

        portfolio = build_product_portfolio(store=product_store, execution_store=execution_store, now=201.0)

        assert [item["product_id"] for item in portfolio["items"]] == [ovyon["product_id"], oryn["product_id"]]
        by_project = {item["project_id"]: item for item in portfolio["items"]}
        assert by_project["Ovyon"]["flow_control"]["state"] == "needs_direction"
        assert by_project["Ovyon"]["attention_state"] == "needs_attention"
        assert "direction" in by_project["Ovyon"]["attention_reason"]
        assert by_project["OrynWorkspace"]["flow_control"]["state"] == "normal"
    finally:
        execution_store.close()
        product_store.close()


def test_portfolio_computes_vision_alignment_from_goal_link_evidence(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    goal_store = DevProjectGoalStore(db_path)
    try:
        def create_linked_subgoal(project_id, title):
            vision = create_project_goal(
                store=goal_store,
                kind="vision",
                title=f"{project_id} vision",
                project_id=project_id,
            )
            goal = create_project_goal(
                store=goal_store,
                kind="goal",
                title=f"{project_id} goal",
                project_id=project_id,
                parent_goal_id=vision["goal_id"],
            )
            milestone = create_project_goal(
                store=goal_store,
                kind="milestone",
                title=f"{project_id} milestone",
                project_id=project_id,
                parent_goal_id=goal["goal_id"],
            )
            return create_project_goal(
                store=goal_store,
                kind="subgoal",
                title=title,
                project_id=project_id,
                parent_goal_id=milestone["goal_id"],
            )

        on_track = create_product(store=product_store, project_id="OnTrack", name="On Track", lifecycle_state="active")
        at_risk = create_product(store=product_store, project_id="AtRisk", name="At Risk", lifecycle_state="active")
        off_track = create_product(store=product_store, project_id="OffTrack", name="Off Track", lifecycle_state="active")
        unassessed = create_product(store=product_store, project_id="Unassessed", name="Unassessed", lifecycle_state="active")

        on_track_goal = create_linked_subgoal("OnTrack", "Advance intended direction")
        execution_store.create_plan(
            title="On-track linked work",
            vision_brief=None,
            tasks=[{
                "task_id": "linked-planned",
                "goal": "Advance intended direction",
                "prompt": "Advance intended direction",
                "project_id": "OnTrack",
                "linked_goal_id": on_track_goal["goal_id"],
            }],
        )

        at_risk_goal = create_linked_subgoal("AtRisk", "Blocked intended direction")
        risk_plan = execution_store.create_plan(
            title="At-risk linked work",
            vision_brief=None,
            tasks=[{
                "task_id": "linked-failed",
                "goal": "Blocked intended direction",
                "prompt": "Blocked intended direction",
                "project_id": "AtRisk",
                "linked_goal_id": at_risk_goal["goal_id"],
            }],
        )
        execution_store.update_task_launch(
            plan_id=risk_plan["plan_id"],
            task_id="linked-failed",
            ao_session_id="ao-linked-failed",
            status="failed",
        )

        off_plan = execution_store.create_plan(
            title="Off-track unlinked work",
            vision_brief=None,
            tasks=[{
                "task_id": "unlinked-failed",
                "goal": "Unlinked failure",
                "prompt": "Unlinked failure",
                "project_id": "OffTrack",
            }],
        )
        execution_store.update_task_launch(
            plan_id=off_plan["plan_id"],
            task_id="unlinked-failed",
            ao_session_id="ao-unlinked-failed",
            status="failed",
        )

        portfolio = build_product_portfolio(
            store=product_store,
            execution_store=execution_store,
            goal_store=goal_store,
            now=201.0,
        )

        by_project = {item["project_id"]: item for item in portfolio["items"]}
        assert by_project["OnTrack"]["product_id"] == on_track["product_id"]
        assert by_project["OnTrack"]["vision_alignment"]["state"] == "on_track"
        assert by_project["OnTrack"]["vision_alignment"]["counts"]["linked"] == 1
        assert by_project["OnTrack"]["vision_alignment"]["counts"]["linked_goal_count"] == 1
        assert by_project["AtRisk"]["product_id"] == at_risk["product_id"]
        assert by_project["AtRisk"]["vision_alignment"]["state"] == "at_risk"
        assert by_project["AtRisk"]["vision_alignment"]["counts"]["linked_risk"] == 1
        assert by_project["OffTrack"]["product_id"] == off_track["product_id"]
        assert by_project["OffTrack"]["vision_alignment"]["state"] == "off_track"
        assert by_project["OffTrack"]["vision_alignment"]["counts"]["unlinked_risk"] == 1
        assert by_project["Unassessed"]["product_id"] == unassessed["product_id"]
        assert by_project["Unassessed"]["vision_alignment"]["state"] == "unassessed"
        assert by_project["Unassessed"]["vision_alignment"]["source"] == "insufficient_evidence"
    finally:
        goal_store.close()
        execution_store.close()
        product_store.close()


def test_product_action_surface_derives_core_action_sources(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    clarification_store = DevClarificationStore(db_path)
    try:
        product = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="needs_direction", reason="Pick next milestone", now=200.0)
        start_clarification(
            store=clarification_store,
            project_id="OrynWorkspace",
            vision_brief="Clarify next Product move",
        )
        plan = execution_store.create_plan(
            title="Draft Product work",
            vision_brief=None,
            tasks=[{
                "task_id": "task-draft",
                "goal": "Review draft",
                "prompt": "Review draft",
                "project_id": "OrynWorkspace",
            }],
        )
        execution_store.create_draft_review(
            plan_id=plan["plan_id"],
            plan_artifact_id="artifact-1",
            build_id="build-1",
        )
        execution_store.create_or_reuse_supervisor_approval(
            plan_id=plan["plan_id"],
            task_ids=["task-draft"],
            recommended_action="retry",
            reason="Worker failed and needs approval.",
            suggested_instruction=None,
            action_overrides={},
            payload={},
        )
        execution_store.update_task_launch(
            plan_id=plan["plan_id"],
            task_id="task-draft",
            ao_session_id="ao-draft",
            status="failed",
        )

        surface = build_product_action_surface(
            store=product_store,
            execution_store=execution_store,
            clarification_store=clarification_store,
        )

        kinds = {action["kind"] for action in surface["data"]}
        assert "direction_needed" in kinds
        assert "clarification" in kinds
        assert "draft_review" in kinds
        assert "supervisor_approval" in kinds
        assert "backlog_attention" in kinds
        assert surface["counts"]["critical"] >= 2
        assert surface["data"][0]["priority"] == "critical"
        assert all(action["product_id"] == product["product_id"] for action in surface["data"])
    finally:
        clarification_store.close()
        execution_store.close()
        product_store.close()


def test_portfolio_includes_product_action_summary(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    clarification_store = DevClarificationStore(db_path)
    try:
        product = create_product(store=product_store, project_id="OrynWorkspace", name="Oryn", lifecycle_state="active")
        product_store.update_flow_control(product["product_id"], state="needs_direction", reason="Pick next milestone", now=200.0)

        portfolio = build_product_portfolio(
            store=product_store,
            execution_store=execution_store,
            clarification_store=clarification_store,
            now=201.0,
        )

        item = portfolio["items"][0]
        assert item["action_counts"]["total"] == 1
        assert item["next_action"]["kind"] == "direction_needed"
        assert item["next_action"]["reason"] == "Pick next milestone"
    finally:
        clarification_store.close()
        execution_store.close()
        product_store.close()


def test_portfolio_sorts_paused_or_held_before_normal_active(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        normal = create_product(store=product_store, project_id="normal", name="Normal", lifecycle_state="active")
        held = create_product(store=product_store, project_id="held", name="Held", lifecycle_state="active")
        product_store.update_flow_control(held["product_id"], state="hold_new_work", now=200.0)

        portfolio = build_product_portfolio(store=product_store, execution_store=execution_store, now=201.0)

        assert [item["product_id"] for item in portfolio["items"]] == [held["product_id"], normal["product_id"]]
        assert portfolio["items"][0]["flow_control"]["state"] == "hold_new_work"
        assert portfolio["items"][0]["attention_reason"] == "Product flow is holding new work."
    finally:
        execution_store.close()
        product_store.close()


def test_portfolio_uses_stable_tie_breaks(tmp_path):
    db_path = tmp_path / "state.db"
    product_store = DevProductStore(db_path)
    execution_store = DevExecutionStore(db_path)
    try:
        create_product(store=product_store, project_id="zeta", name="Zeta", lifecycle_state="planned")
        create_product(store=product_store, project_id="alpha", name="Alpha", lifecycle_state="planned")

        portfolio = build_product_portfolio(
            store=product_store,
            execution_store=execution_store,
            now=123.0,
        )

        assert [item["name"] for item in portfolio["items"]] == ["Alpha", "Zeta"]
        assert [item["ordering"] for item in portfolio["items"]] == [1, 2]
    finally:
        execution_store.close()
        product_store.close()


def test_product_live_snapshot_includes_detail_sources_and_is_read_only(tmp_path):
    product_store = DevProductStore(tmp_path / "state.db")
    execution_store = DevExecutionStore(tmp_path / "state.db")
    try:
        product = create_product(
            store=product_store,
            project_id="OrynWorkspace",
            name="Oryn",
            lifecycle_state="active",
        )
        plan = execution_store.create_plan(
            title="Live Product detail",
            vision_brief=None,
            tasks=[{
                "task_id": "task-detail",
                "goal": "Expose Product detail stream",
                "prompt": "Expose Product detail stream",
                "project_id": "OrynWorkspace",
            }],
        )
        product_store.update_flow_control(
            product["product_id"],
            state="needs_direction",
            reason="Choose next Product slice",
            now=123.0,
        )
        tick_product_progression_loop(
            store=product_store,
            execution_store=execution_store,
            product_id=product["product_id"],
            now=124.0,
        )
        refreshed = product_store.get(product["product_id"])

        snapshot = build_product_live_snapshot(
            store=product_store,
            product=refreshed,
            execution_store=execution_store,
            now=125.0,
        )

        assert snapshot["object"] == "hermes.dev_product_live_snapshot"
        assert snapshot["event"] == "dev.product.snapshot"
        assert snapshot["product"]["product_id"] == product["product_id"]
        assert snapshot["backlog"]["items"][0]["source"]["task_id"] == "task-detail"
        assert snapshot["actions"]["data"][0]["kind"] == "direction_needed"
        assert snapshot["actions"]["data"][0]["product_id"] == product["product_id"]
        assert snapshot["progression_loop"]["data"][0]["status"] == "held_by_flow_control"
        assert snapshot["freshness"]["backlog_count"] == 1
        assert snapshot["freshness"]["action_count"] == 1
        assert snapshot["freshness"]["progression_count"] == 1
        assert {source["kind"] for source in snapshot["sources"]} == {
            "dev_product",
            "dev_product_backlog",
            "dev_product_actions",
            "dev_product_progression_loop",
        }
        assert execution_store.list_launch_records(plan["plan_id"]) == []
    finally:
        execution_store.close()
        product_store.close()
