import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.dev_control.clarifications import start_clarification
from gateway.dev_control.routes import register_dev_control_routes
from gateway.dev_execution import DevExecutionStore
from gateway.platforms.api_server import APIServerAdapter, cors_middleware, security_headers_middleware
from gateway.config import PlatformConfig


def _app(tmp_path):
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "sk-secret"}))
    adapter._dev_execution_store = DevExecutionStore(tmp_path / "state.db")
    mws = [mw for mw in (cors_middleware, security_headers_middleware) if mw is not None]
    app = web.Application(middlewares=mws)
    register_dev_control_routes(app, adapter)
    return app, adapter


async def _read_sse_events(response):
    text = await response.text()
    events = []
    current = {}
    for line in text.splitlines():
        if not line.strip():
            if current:
                events.append(current)
                current = {}
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event: "):
            current["event"] = line.removeprefix("event: ").strip()
        elif line.startswith("data: "):
            current.setdefault("data", "")
            current["data"] += line.removeprefix("data: ").strip()
    if current:
        events.append(current)
    return events


@pytest.mark.asyncio
async def test_products_api_requires_auth(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            response = await cli.get("/v1/dev/products")
            assert response.status in {401, 403}
            portfolio_response = await cli.get("/v1/dev/products/portfolio")
            assert portfolio_response.status in {401, 403}
            stream_response = await cli.get("/v1/dev/products/portfolio/stream?once=true")
            assert stream_response.status in {401, 403}
            portfolio_flow_response = await cli.post("/v1/dev/products/portfolio/flow-control", json={"state": "paused"})
            assert portfolio_flow_response.status in {401, 403}
            detail_stream_response = await cli.get("/v1/dev/products/product-missing/stream?once=true")
            assert detail_stream_response.status in {401, 403}
            actions_response = await cli.get("/v1/dev/products/actions")
            assert actions_response.status in {401, 403}
            loop_response = await cli.get("/v1/dev/products/progression-loop")
            assert loop_response.status in {401, 403}
            tick_response = await cli.post("/v1/dev/products/progression-loop/tick", json={})
            assert tick_response.status in {401, 403}
            flow_response = await cli.post("/v1/dev/products/product-missing/flow-control", json={"state": "paused"})
            assert flow_response.status in {401, 403}
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_api_create_list_detail_and_backlog(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={
                    "project_id": "OrynWorkspace",
                    "name": "Oryn Workspace",
                    "lifecycle_state": "active",
                    "repository_bindings": [{"path": "/repo/oryn"}],
                },
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert created_resp.status == 200
            created = await created_resp.json()

            adapter._dev_execution_store.create_plan(
                title="Product API slice",
                vision_brief=None,
                tasks=[{
                    "task_id": "task-api",
                    "goal": "Expose Product API",
                    "prompt": "Expose Product API",
                    "project_id": "OrynWorkspace",
                }],
            )

            list_resp = await cli.get(
                "/v1/dev/products?project_id=OrynWorkspace",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert list_resp.status == 200
            listed = await list_resp.json()
            assert listed["total"] == 1
            assert listed["data"][0]["product_id"] == created["product_id"]

            detail_resp = await cli.get(
                f"/v1/dev/products/{created['product_id']}?include_backlog=1",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert detail_resp.status == 200
            detail = await detail_resp.json()
            assert detail["product_id"] == created["product_id"]
            assert detail["backlog"]["items"][0]["source"]["task_id"] == "task-api"

            backlog_resp = await cli.get(
                f"/v1/dev/products/{created['product_id']}/backlog",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert backlog_resp.status == 200
            backlog = await backlog_resp.json()
            assert backlog["object"] == "hermes.dev_product_backlog"
            assert backlog["counts"]["planned"] == 1
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_portfolio_api_returns_attention_ordered_summaries(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            oryn_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            ovyon_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "Ovyon", "name": "Ovyon", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert oryn_resp.status == 200
            assert ovyon_resp.status == 200
            oryn = await oryn_resp.json()
            ovyon = await ovyon_resp.json()

            oryn_plan = adapter._dev_execution_store.create_plan(
                title="Oryn failed work",
                vision_brief=None,
                tasks=[{
                    "task_id": "oryn-failed",
                    "goal": "Needs attention",
                    "prompt": "Needs attention",
                    "project_id": "OrynWorkspace",
                }],
            )
            adapter._dev_execution_store.update_task_launch(
                plan_id=oryn_plan["plan_id"],
                task_id="oryn-failed",
                ao_session_id="ao-oryn",
                status="failed",
            )
            ovyon_plan = adapter._dev_execution_store.create_plan(
                title="Ovyon running work",
                vision_brief=None,
                tasks=[{
                    "task_id": "ovyon-running",
                    "goal": "Running",
                    "prompt": "Running",
                    "project_id": "Ovyon",
                }],
            )
            adapter._dev_execution_store.update_task_launch(
                plan_id=ovyon_plan["plan_id"],
                task_id="ovyon-running",
                ao_session_id="ao-ovyon",
                status="launched",
            )

            response = await cli.get(
                "/v1/dev/products/portfolio",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert response.status == 200
            payload = await response.json()

            assert payload["object"] == "hermes.dev_product_portfolio"
            assert payload["total"] == 2
            assert payload["items"][0]["product_id"] == oryn["product_id"]
            assert payload["items"][0]["attention_state"] == "needs_attention"
            assert payload["items"][0]["next_item"]["source"]["task_id"] == "oryn-failed"
            assert payload["items"][1]["product_id"] == ovyon["product_id"]
            assert payload["items"][1]["attention_state"] == "active"

            detail_response = await cli.get(
                f"/v1/dev/products/{oryn['product_id']}",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert detail_response.status == 200
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_flow_control_api_updates_and_rejects_invalid_states(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert created_resp.status == 200
            created = await created_resp.json()

            update_resp = await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "paused", "autonomy_level": "manual", "reason": "Reframe", "requested_by": "Felipe"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert update_resp.status == 200
            updated_payload = await update_resp.json()
            assert updated_payload["object"] == "hermes.dev_product_flow_control_update"
            assert updated_payload["flow_control"]["state"] == "paused"
            assert updated_payload["flow_control"]["autonomy_level"] == "manual"
            assert updated_payload["flow_control"]["reason"] == "Reframe"
            assert updated_payload["product"]["flow_control"]["requested_by"] == "Felipe"

            portfolio_resp = await cli.get(
                "/v1/dev/products/portfolio",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert portfolio_resp.status == 200
            portfolio = await portfolio_resp.json()
            assert portfolio["items"][0]["flow_control"]["state"] == "paused"

            invalid_resp = await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "release"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert invalid_resp.status == 400

            detail_resp = await cli.get(
                f"/v1/dev/products/{created['product_id']}",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert detail_resp.status == 200
            detail = await detail_resp.json()
            assert detail["flow_control"]["state"] == "paused"
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_portfolio_flow_control_api_updates_and_surfaces_in_portfolio(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert created_resp.status == 200

            update_resp = await cli.post(
                "/v1/dev/products/portfolio/flow-control",
                json={"state": "hold_new_work", "autonomy_level": "manual", "reason": "Budget review", "requested_by": "Felipe"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert update_resp.status == 200
            updated = await update_resp.json()
            assert updated["object"] == "hermes.dev_portfolio_flow_control_update"
            assert updated["flow_control"]["state"] == "hold_new_work"
            assert updated["flow_control"]["autonomy_level"] == "manual"
            assert updated["flow_control"]["reason"] == "Budget review"

            portfolio_resp = await cli.get(
                "/v1/dev/products/portfolio",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert portfolio_resp.status == 200
            portfolio = await portfolio_resp.json()
            assert portfolio["flow_control"]["state"] == "hold_new_work"
            assert portfolio["flow_control"]["autonomy_level"] == "manual"
            assert portfolio["items"][0]["flow_control"]["state"] == "normal"

            stream_resp = await cli.get(
                "/v1/dev/products/portfolio/stream?once=true",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )
            events = await _read_sse_events(stream_resp)
            snapshot = json.loads(events[0]["data"])
            assert snapshot["flow_control"]["state"] == "hold_new_work"
            assert snapshot["portfolio"]["flow_control"]["autonomy_level"] == "manual"

            invalid_resp = await cli.post(
                "/v1/dev/products/portfolio/flow-control",
                json={"state": "normal", "autonomy_level": "self_merge"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert invalid_resp.status == 400

            after_invalid_resp = await cli.get(
                "/v1/dev/products/portfolio",
                headers={"Authorization": "Bearer sk-secret"},
            )
            after_invalid = await after_invalid_resp.json()
            assert after_invalid["flow_control"]["state"] == "hold_new_work"
            assert after_invalid["flow_control"]["autonomy_level"] == "manual"
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_portfolio_flow_control_blocks_new_work_and_resume_allows_it(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert created_resp.status == 200

            pause_resp = await cli.post(
                "/v1/dev/products/portfolio/flow-control",
                json={"state": "paused", "reason": "Portfolio pause", "requested_by": "Felipe"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert pause_resp.status == 200

            blocked_resp = await cli.post(
                "/v1/dev/execution-plans",
                json={
                    "title": "Should not start",
                    "tasks": [{
                        "task_id": "task-blocked",
                        "goal": "Blocked",
                        "prompt": "Blocked",
                        "project_id": "OrynWorkspace",
                    }],
                },
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert blocked_resp.status == 409
            blocked = await blocked_resp.json()
            assert blocked["flow_control_gate"]["object"] == "hermes.dev_portfolio_flow_control_gate"
            assert blocked["flow_control_gate"]["state"] == "paused"
            assert adapter._dev_execution_store.list_plans(project_id="OrynWorkspace") == []

            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert tick_resp.status == 200
            tick_payload = await tick_resp.json()
            assert tick_payload["data"][0]["status"] == "held_by_flow_control"
            assert tick_payload["data"][0]["portfolio_flow_control"]["state"] == "paused"

            resume_resp = await cli.post(
                "/v1/dev/products/portfolio/flow-control",
                json={"state": "normal", "autonomy_level": "bounded", "reason": "Resume bounded work"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert resume_resp.status == 200

            allowed_resp = await cli.post(
                "/v1/dev/execution-plans",
                json={
                    "title": "Allowed after resume",
                    "tasks": [{
                        "task_id": "task-allowed",
                        "goal": "Allowed",
                        "prompt": "Allowed",
                        "project_id": "OrynWorkspace",
                    }],
                },
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert allowed_resp.status == 200
            assert len(adapter._dev_execution_store.list_plans(project_id="OrynWorkspace")) == 1
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_actions_api_returns_read_only_product_actions(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "needs_direction", "reason": "Choose next bet"},
                headers={"Authorization": "Bearer sk-secret"},
            )

            response = await cli.get(
                "/v1/dev/products/actions",
                headers={"Authorization": "Bearer sk-secret"},
            )

            assert response.status == 200
            payload = await response.json()
            assert payload["object"] == "hermes.dev_product_actions"
            assert payload["total"] == 1
            assert payload["data"][0]["kind"] == "direction_needed"
            assert payload["data"][0]["product_id"] == created["product_id"]
            assert adapter._dev_execution_store.list_plans() == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_portfolio_stream_one_shot_empty_snapshot(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            response = await cli.get(
                "/v1/dev/products/portfolio/stream?once=true",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )

            assert response.status == 200
            assert response.headers["Content-Type"].startswith("text/event-stream")
            events = await _read_sse_events(response)
            assert len(events) == 1
            assert events[0]["event"] == "dev.product.portfolio.snapshot"
            payload = json.loads(events[0]["data"])
            assert payload["object"] == "hermes.dev_product_live_portfolio_snapshot"
            assert payload["portfolio"]["total"] == 0
            assert payload["actions"]["total"] == 0
            assert payload["progression_loop"]["total"] == 0
            assert payload["freshness"]["product_count"] == 0
            assert adapter._dev_execution_store.list_plans() == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_portfolio_stream_includes_actions_progression_and_freshness(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "needs_direction", "reason": "Choose next slice"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={"product_id": created["product_id"]},
                headers={"Authorization": "Bearer sk-secret"},
            )
            tick_payload = await tick_resp.json()
            assert tick_payload["evaluated_count"] == 1

            response = await cli.get(
                "/v1/dev/products/portfolio/stream?once=true",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )
            events = await _read_sse_events(response)
            payload = json.loads(events[0]["data"])

            assert payload["event"] == "dev.product.portfolio.snapshot"
            assert payload["sequence"] == 1
            assert payload["portfolio"]["items"][0]["product_id"] == created["product_id"]
            assert payload["actions"]["data"][0]["kind"] == "direction_needed"
            assert payload["progression_loop"]["data"][0]["status"] == "held_by_flow_control"
            assert payload["freshness"]["portfolio_updated_at"] is not None
            assert payload["freshness"]["actions_updated_at"] is not None
            assert payload["freshness"]["progression_updated_at"] is not None
            assert {source["kind"] for source in payload["sources"]} == {
                "dev_product_portfolio",
                "dev_product_actions",
                "dev_product_progression_loop",
            }
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_portfolio_stream_bounded_max_events_and_no_side_effects(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            plan = adapter._dev_execution_store.create_plan(
                title="Stream must not launch",
                vision_brief=None,
                tasks=[{
                    "task_id": "task-stream",
                    "goal": "Remain planned",
                    "prompt": "Remain planned",
                    "project_id": "OrynWorkspace",
                }],
            )

            response = await cli.get(
                "/v1/dev/products/portfolio/stream?max_events=2&snapshot_interval=0.01",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )
            events = await _read_sse_events(response)
            snapshots = [event for event in events if event.get("event") == "dev.product.portfolio.snapshot"]
            payloads = [json.loads(event["data"]) for event in snapshots]
            progression_resp = await cli.get(
                f"/v1/dev/products/progression-loop?product_id={created['product_id']}",
                headers={"Authorization": "Bearer sk-secret"},
            )
            progression = await progression_resp.json()

            assert len(snapshots) == 2
            assert [payload["sequence"] for payload in payloads] == [1, 2]
            assert all(payload["portfolio"]["total"] == 1 for payload in payloads)
            assert progression["total"] == 0
            assert adapter._dev_execution_store.list_launch_records(plan["plan_id"]) == []
            assert len(adapter._dev_execution_store.list_plans(project_id="OrynWorkspace")) == 1
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_product_detail_stream_includes_backlog_actions_progression_and_freshness(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            plan = adapter._dev_execution_store.create_plan(
                title="Product detail stream",
                vision_brief=None,
                tasks=[{
                    "task_id": "task-detail-stream",
                    "goal": "Stream selected Product detail",
                    "prompt": "Stream selected Product detail",
                    "project_id": "OrynWorkspace",
                }],
            )
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "needs_direction", "reason": "Pick next slice"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={"product_id": created["product_id"]},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert tick_resp.status == 200

            response = await cli.get(
                f"/v1/dev/products/{created['product_id']}/stream?once=true",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )
            assert response.status == 200
            assert response.headers["Content-Type"].startswith("text/event-stream")
            events = await _read_sse_events(response)
            payload = json.loads(events[0]["data"])

            assert events[0]["event"] == "dev.product.snapshot"
            assert payload["object"] == "hermes.dev_product_live_snapshot"
            assert payload["sequence"] == 1
            assert payload["product"]["product_id"] == created["product_id"]
            assert payload["backlog"]["items"][0]["source"]["task_id"] == "task-detail-stream"
            assert payload["actions"]["data"][0]["kind"] == "direction_needed"
            assert payload["actions"]["data"][0]["product_id"] == created["product_id"]
            assert payload["progression_loop"]["data"][0]["status"] == "held_by_flow_control"
            assert payload["freshness"]["product_updated_at"] is not None
            assert payload["freshness"]["backlog_updated_at"] is not None
            assert payload["freshness"]["actions_updated_at"] is not None
            assert payload["freshness"]["progression_updated_at"] is not None
            assert {source["kind"] for source in payload["sources"]} == {
                "dev_product",
                "dev_product_backlog",
                "dev_product_actions",
                "dev_product_progression_loop",
            }
            assert adapter._dev_execution_store.list_launch_records(plan["plan_id"]) == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_product_detail_stream_project_lookup_missing_and_bounded_events(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            missing_resp = await cli.get(
                "/v1/dev/products/product-missing/stream?once=true",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )
            assert missing_resp.status == 404

            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            adapter._dev_execution_store.create_plan(
                title="Project lookup stream",
                vision_brief=None,
                tasks=[{
                    "task_id": "task-project-lookup",
                    "goal": "Stream by project id",
                    "prompt": "Stream by project id",
                    "project_id": "OrynWorkspace",
                }],
            )

            response = await cli.get(
                "/v1/dev/products/project:OrynWorkspace/stream?max_events=2&snapshot_interval=0.01",
                headers={"Authorization": "Bearer sk-secret", "Accept": "text/event-stream"},
            )
            assert response.status == 200
            events = await _read_sse_events(response)
            snapshots = [event for event in events if event.get("event") == "dev.product.snapshot"]
            payloads = [json.loads(event["data"]) for event in snapshots]
            progression_resp = await cli.get(
                f"/v1/dev/products/progression-loop?product_id={created['product_id']}",
                headers={"Authorization": "Bearer sk-secret"},
            )
            progression = await progression_resp.json()

            assert len(snapshots) == 2
            assert [payload["sequence"] for payload in payloads] == [1, 2]
            assert all(payload["product"]["product_id"] == created["product_id"] for payload in payloads)
            assert all(payload["backlog"]["counts"]["planned"] == 1 for payload in payloads)
            assert progression["total"] == 0
            assert len(adapter._dev_execution_store.list_plans(project_id="OrynWorkspace")) == 1
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_progression_loop_api_ticks_and_reads_latest_state(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            plan = adapter._dev_execution_store.create_plan(
                title="Product loop work",
                vision_brief=None,
                tasks=[{
                    "task_id": "task-loop",
                    "goal": "Advance",
                    "prompt": "Advance",
                    "project_id": "OrynWorkspace",
                }],
            )
            before_plans = adapter._dev_execution_store.list_plans(project_id="OrynWorkspace")

            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={"product_id": created["product_id"]},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert tick_resp.status == 200
            tick = await tick_resp.json()
            state_resp = await cli.get(
                "/v1/dev/products/progression-loop",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert state_resp.status == 200
            state = await state_resp.json()

            assert tick["object"] == "hermes.dev_product_progression_loop_tick"
            assert tick["evaluated_count"] == 1
            assert tick["data"][0]["product_id"] == created["product_id"]
            assert tick["data"][0]["status"] == "advanced"
            assert tick["data"][0]["autonomy_policy"]["product_autonomy_level"] == "supervised"
            assert tick["data"][0]["autonomy_policy"]["portfolio_autonomy_level"] == "supervised"
            assert tick["data"][0]["autonomy_policy"]["effective_autonomy_level"] == "supervised"
            assert tick["data"][0]["autonomy_policy"]["decision"] == "advisory_advance"
            assert state["object"] == "hermes.dev_product_progression_loop"
            assert state["total"] == 1
            assert state["data"][0]["iteration_id"] == tick["data"][0]["iteration_id"]
            assert state["data"][0]["autonomy_policy"] == tick["data"][0]["autonomy_policy"]
            assert adapter._dev_execution_store.list_plans(project_id="OrynWorkspace") == before_plans
            assert adapter._dev_execution_store.list_launch_records(plan["plan_id"]) == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_progression_loop_api_flow_hold_is_readable_and_side_effect_safe(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "paused", "reason": "Stop while reframing"},
                headers={"Authorization": "Bearer sk-secret"},
            )

            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert tick_resp.status == 200
            tick = await tick_resp.json()
            filtered_resp = await cli.get(
                f"/v1/dev/products/progression-loop?product_id={created['product_id']}",
                headers={"Authorization": "Bearer sk-secret"},
            )
            filtered = await filtered_resp.json()

            assert tick["counts"]["held_by_flow_control"] == 1
            assert tick["data"][0]["status"] == "held_by_flow_control"
            assert tick["data"][0]["reason"] == "Stop while reframing"
            assert filtered["total"] == 1
            assert filtered["data"][0]["product_id"] == created["product_id"]
            assert adapter._dev_execution_store.list_plans(project_id="OrynWorkspace") == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_progression_loop_api_bounded_launch_preserves_draft_gate(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "DraftGate", "name": "Draft Gate", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "normal", "autonomy_level": "bounded"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            await cli.post(
                "/v1/dev/products/portfolio/flow-control",
                json={"state": "normal", "autonomy_level": "bounded"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            plan = adapter._dev_execution_store.create_plan(
                title="Draft gated Product work",
                vision_brief=None,
                tasks=[{
                    "task_id": "draft-gated",
                    "goal": "Wait for draft",
                    "prompt": "Wait for draft",
                    "project_id": "DraftGate",
                }],
            )
            adapter._dev_execution_store.create_draft_review(
                plan_id=plan["plan_id"],
                plan_artifact_id="artifact-draft",
                build_id="build-draft",
            )

            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={"product_id": created["product_id"]},
                headers={"Authorization": "Bearer sk-secret"},
            )
            tick = await tick_resp.json()
            latest_plan = adapter._dev_execution_store.get_plan(plan["plan_id"])

            assert tick_resp.status == 200
            assert tick["data"][0]["status"] == "waiting_for_human"
            assert tick["data"][0]["selected_action_kind"] == "draft_review"
            assert tick["data"][0]["autonomy_policy"]["decision"] == "human_action_required"
            assert "transition" not in tick["data"][0]
            assert latest_plan["tasks"][0]["status"] == "planned"
            assert latest_plan["tasks"][0]["ao_session_id"] is None
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_progression_loop_api_human_gate_blocks_bounded_verification(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "VerifyHumanGate", "name": "Verify Human Gate", "lifecycle_state": "active"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "normal", "autonomy_level": "bounded"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            await cli.post(
                "/v1/dev/products/portfolio/flow-control",
                json={"state": "normal", "autonomy_level": "bounded"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            plan = adapter._dev_execution_store.create_plan(
                title="Completed gated Product work",
                vision_brief=None,
                tasks=[{
                    "task_id": "complete-gated",
                    "goal": "Wait for human before verification",
                    "prompt": "Wait for human before verification",
                    "project_id": "VerifyHumanGate",
                    "acceptance_criteria": [{
                        "statement": "Tests pass",
                        "verification_method": "command",
                        "verification_detail": "make test",
                        "machine_checkable": True,
                    }],
                }],
            )
            adapter._dev_execution_store.update_task_launch(
                plan_id=plan["plan_id"],
                task_id="complete-gated",
                ao_session_id="ao-completed",
                status="completed",
            )
            start_clarification(
                store=adapter._ensure_dev_clarification_store(),
                project_id="VerifyHumanGate",
                vision_brief="Choose verification direction",
            )

            tick_resp = await cli.post(
                "/v1/dev/products/progression-loop/tick",
                json={"product_id": created["product_id"]},
                headers={"Authorization": "Bearer sk-secret"},
            )
            tick = await tick_resp.json()
            verification_store = adapter._ensure_dev_verification_store()

            assert tick_resp.status == 200
            assert tick["data"][0]["status"] == "waiting_for_human"
            assert tick["data"][0]["selected_action_kind"] == "clarification"
            assert tick["data"][0]["autonomy_policy"]["decision"] == "human_action_required"
            assert "transition" not in tick["data"][0]
            assert verification_store.list_runs(plan_id=plan["plan_id"], task_id="complete-gated") == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_product_flow_control_blocks_direct_plan_creation(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            flow_resp = await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "paused", "reason": "Pause before replanning"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert flow_resp.status == 200

            plan_resp = await cli.post(
                "/v1/dev/execution-plans",
                json={
                    "title": "Blocked Product work",
                    "tasks": [{
                        "task_id": "task-blocked",
                        "goal": "Should not start",
                        "prompt": "Should not start",
                        "project_id": "OrynWorkspace",
                    }],
                },
                headers={"Authorization": "Bearer sk-secret"},
            )

            assert plan_resp.status == 409
            payload = await plan_resp.json()
            assert payload["error"]["code"] == "product_flow_control_blocked"
            assert payload["flow_control_gate"]["product_id"] == created["product_id"]
            assert payload["flow_control_gate"]["project_id"] == "OrynWorkspace"
            assert payload["flow_control_gate"]["state"] == "paused"
            assert adapter._dev_execution_store.list_plans(project_id="OrynWorkspace") == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_product_flow_control_blocks_launch_without_launch_record(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            plan = adapter._dev_execution_store.create_plan(
                title="Held Product work",
                vision_brief=None,
                tasks=[{
                    "task_id": "task-held",
                    "goal": "Hold",
                    "prompt": "Hold",
                    "project_id": "OrynWorkspace",
                }],
            )
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "hold_new_work"},
                headers={"Authorization": "Bearer sk-secret"},
            )

            launch_resp = await cli.post(
                f"/v1/dev/execution-plans/{plan['plan_id']}/launch",
                json={"task_ids": ["task-held"]},
                headers={"Authorization": "Bearer sk-secret"},
            )

            assert launch_resp.status == 409
            payload = await launch_resp.json()
            assert payload["flow_control_gate"]["state"] == "hold_new_work"
            assert adapter._dev_execution_store.list_launch_records(plan["plan_id"]) == []

            detail_resp = await cli.get(
                f"/v1/dev/execution-plans/{plan['plan_id']}",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert detail_resp.status == 200
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_product_flow_control_blocks_artifact_execution_plan_creation(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            artifact_store = adapter._ensure_dev_plan_artifact_store()
            artifact = artifact_store.create({
                "plan_artifact_id": "artifact-flow-gated",
                "clarification_id": "clarification-flow-gated",
                "project_id": "OrynWorkspace",
                "session_id": None,
                "status": "approved",
                "version": 1,
                "source": "test",
                "title": "Approved but gated",
                "markdown": "# Approved but gated",
                "payload": {"recommended_slices": []},
                "revision_history": [],
                "superseded_by": None,
                "approved_at": 123.0,
                "cancelled_at": None,
            })
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "needs_direction"},
                headers={"Authorization": "Bearer sk-secret"},
            )

            build_resp = await cli.post(
                f"/v1/dev/plan-artifacts/{artifact['plan_artifact_id']}/create-execution-plan",
                json={},
                headers={"Authorization": "Bearer sk-secret"},
            )

            assert build_resp.status == 409
            payload = await build_resp.json()
            assert payload["flow_control_gate"]["state"] == "needs_direction"
            assert artifact_store.get(artifact["plan_artifact_id"])["status"] == "approved"
            assert adapter._dev_execution_store.list_plans(project_id="OrynWorkspace") == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_product_flow_control_normal_allows_direct_plan_creation(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            created_resp = await cli.post(
                "/v1/dev/products",
                json={"project_id": "OrynWorkspace", "name": "Oryn"},
                headers={"Authorization": "Bearer sk-secret"},
            )
            created = await created_resp.json()
            await cli.post(
                f"/v1/dev/products/{created['product_id']}/flow-control",
                json={"state": "normal"},
                headers={"Authorization": "Bearer sk-secret"},
            )

            plan_resp = await cli.post(
                "/v1/dev/execution-plans",
                json={
                    "title": "Allowed Product work",
                    "tasks": [{
                        "task_id": "task-allowed",
                        "goal": "Allowed",
                        "prompt": "Allowed",
                        "project_id": "OrynWorkspace",
                    }],
                },
                headers={"Authorization": "Bearer sk-secret"},
            )

            assert plan_resp.status == 200
            payload = await plan_resp.json()
            assert payload["ok"] is True
            assert payload["plan"]["tasks"][0]["task_id"] == "task-allowed"
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_products_portfolio_api_empty_payload_is_read_only(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        async with TestClient(TestServer(app)) as cli:
            response = await cli.get(
                "/v1/dev/products/portfolio",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert response.status == 200
            payload = await response.json()
            assert payload["total"] == 0
            assert payload["items"] == []
            assert adapter._dev_execution_store.list_plans() == []
    finally:
        adapter._dev_execution_store.close()


@pytest.mark.asyncio
async def test_project_dashboard_includes_optional_product_spine(tmp_path):
    app, adapter = _app(tmp_path)
    try:
        adapter._dev_execution_store.create_plan(
            title="Dashboard Product slice",
            vision_brief=None,
            tasks=[{
                "task_id": "task-dashboard",
                "goal": "Show Product on dashboard",
                "prompt": "Show Product on dashboard",
                "project_id": "OrynWorkspace",
            }],
        )
        async with TestClient(TestServer(app)) as cli:
            response = await cli.get(
                "/v1/oryn/project-dashboard?project_id=OrynWorkspace",
                headers={"Authorization": "Bearer sk-secret"},
            )
            assert response.status == 200
            payload = await response.json()
            assert payload["product"]["project_id"] == "OrynWorkspace"
            assert payload["product_backlog"]["items"][0]["source"]["task_id"] == "task-dashboard"
            assert payload["dev_plans"]
    finally:
        adapter._dev_execution_store.close()
