import json
import time
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.dev_control.answer_feedback import (
    DevAnswerFeedbackStore,
    answer_feedback_trace_id,
    normalize_answer_feedback,
    redact_answer_feedback_text,
)
from gateway.dev_control.laminar_exporter import export_answer_feedback_event, export_answer_judge_event
from gateway.dev_execution import DevExecutionStore
from gateway.platforms.api_server import APIServerAdapter, cors_middleware, security_headers_middleware


def _feedback(**overrides):
    payload = {
        "event_id": f"feedback-{time.time_ns()}",
        "client_ts": time.time(),
        "session_id": "session-1",
        "message_id": "message-1",
        "profile": "ovyon",
        "mode": "work",
        "model": "gpt-test",
        "rating": "down",
        "reason_tags": ["missing_context", "too_vague"],
        "comment": "It missed Bearer sk-secret1234567890 and /Users/felipe/file.txt",
        "user_prompt_excerpt": "What needs attention?",
        "answer_excerpt": "The answer listed ids but no labels.",
        "awareness_packet_id": "packet-1",
        "awareness_object_refs": ["approval-1"],
        "context": {"source": "manual", "user_text": "drop this"},
    }
    payload.update(overrides)
    return payload


def test_normalize_answer_feedback_redacts_and_derives_trace_id():
    event = normalize_answer_feedback(_feedback(trace_id="", reason_tags=["missing_context", "not-real"]))

    assert event["trace_id"] == answer_feedback_trace_id("session-1", "message-1")
    assert event["reason_tags"] == ["missing_context"]
    assert "[REDACTED]" in event["comment_redacted"]
    assert "user_text" not in event["context"]


def test_store_preserves_individual_feedback_and_filters(tmp_path):
    store = DevAnswerFeedbackStore(tmp_path / "state.db")
    first = _feedback(event_id="feedback-1", reason_tags=["missing_context"])
    second = _feedback(event_id="feedback-2", reason_tags=["wrong_priority"])

    result = store.ingest_batch({"events": [first, second]})
    missing = store.list_events(profile="ovyon", rating="down", reason="missing_context")

    assert result["accepted"] == 2
    assert len(store.list_events()) == 2
    assert [event["event_id"] for event in missing] == ["feedback-1"]


def test_store_judges_and_exports_ovyon_fixture(tmp_path):
    store = DevAnswerFeedbackStore(tmp_path / "state.db")
    store.ingest_batch({"events": [_feedback(event_id="feedback-1")]})

    judged = store.judge_event("feedback-1", export=lambda event: {"status": "disabled"})
    exported = store.export_ovyon_fixture("feedback-1", tmp_path / "fixtures")

    assert judged["scores"]["judge_context_fit"] < 0.5
    assert Path(exported["path"]).exists()
    fixture = json.loads(Path(exported["path"]).read_text())
    assert fixture["trace_id"] == answer_feedback_trace_id("session-1", "message-1")
    assert fixture["awareness_object_refs"] == ["approval-1"]


def test_laminar_answer_feedback_export_redacts_and_fails_open(monkeypatch):
    event = normalize_answer_feedback(_feedback(event_id="feedback-1"))
    monkeypatch.setenv("HERMES_LAMINAR_EXPORT_ENABLED", "1")
    monkeypatch.setattr(
        "gateway.dev_control.laminar_exporter.urllib.request.urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("down")),
    )

    result = export_answer_feedback_event(event)

    assert result["status"] == "failed_open"


def test_laminar_answer_feedback_export_payload(monkeypatch):
    event = normalize_answer_feedback(_feedback(event_id="feedback-1"))
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data.decode())
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setenv("HERMES_LAMINAR_EXPORT_ENABLED", "1")
    monkeypatch.setattr("gateway.dev_control.laminar_exporter.urllib.request.urlopen", fake_urlopen)

    result = export_answer_judge_event({**event, "judge_scores": {"judge_usefulness": 0.2}})

    span = captured["body"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert result["status"] == "exported"
    assert span["traceId"] == event["trace_id"]
    assert span["name"] == "answer.feedback.judge"


@pytest.mark.asyncio
async def test_answer_feedback_api_ingests_lists_and_judges(tmp_path):
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "sk-secret"}))
    adapter._dev_execution_store = DevExecutionStore(tmp_path / "state.db")
    mws = [mw for mw in (cors_middleware, security_headers_middleware) if mw is not None]
    app = web.Application(middlewares=mws)
    app.router.add_get("/v1/dev/answer-feedback", adapter._handle_dev_answer_feedback)
    app.router.add_post("/v1/dev/answer-feedback", adapter._handle_dev_answer_feedback)
    app.router.add_post("/v1/dev/answer-feedback/{event_id}/judge", adapter._handle_dev_answer_feedback_judge)

    async with TestClient(TestServer(app)) as cli:
        ingest = await cli.post(
            "/v1/dev/answer-feedback",
            json={"events": [_feedback(event_id="feedback-1")]},
            headers={"Authorization": "Bearer sk-secret"},
        )
        listed = await cli.get(
            "/v1/dev/answer-feedback?profile=ovyon&rating=down&reason=missing_context",
            headers={"Authorization": "Bearer sk-secret"},
        )
        judged = await cli.post(
            "/v1/dev/answer-feedback/feedback-1/judge",
            headers={"Authorization": "Bearer sk-secret"},
        )

        assert ingest.status == 200
        assert listed.status == 200
        assert judged.status == 200
        list_data = await listed.json()
        judge_data = await judged.json()

    assert len(list_data["data"]) == 1
    assert judge_data["scores"]["judge_usefulness"] < 0.5


def test_redaction_bounds_sensitive_text():
    redacted = redact_answer_feedback_text(
        "email a@example.com token=abc123 /Users/felipe/secret.txt " + ("x" * 1000),
        120,
    )

    assert "a@example.com" not in redacted
    assert "/Users/felipe" not in redacted
    assert len(redacted) <= 120
