import json
import subprocess
import sys
import time
from pathlib import Path

from gateway.dev_control import production_signals
from gateway.dev_control.acceptance_verification import DevVerificationStore
from gateway.dev_control.lab_loop import DevLabLoopStore, loop_health
from gateway.dev_control.laminar_exporter import export_subagent_event, redact_attributes, trace_attributes_for_event
from gateway.dev_control.production_signals import (
    DevProductionSignalStore,
    generate_signal_report,
    measure_proposal_outcome,
    run_signal_digest_sources,
    signal_health,
    transition_backlog_proposal,
)
from gateway.dev_control.reliability import DevReliabilityStore
from gateway.dev_control.signal_source import DeterministicSignalSource, LaminarSignalSource, SignalWindow, default_thresholds
from gateway.subagent_events import SubagentEventStore


def _event_store(tmp_path):
    return SubagentEventStore(tmp_path / "state.db")


def _append_signal(event_store, **overrides):
    payload = {
        "runtime": "fixture",
        "subagent_id": "worker-1",
        "event": "subagent.completed",
        "status": "failed",
        "summary": "fixture production signal",
        "launch_plan_id": "plan-1",
        "launch_task_id": "task-1",
        "worker_confidence": 0.4,
        "output_contract_score": 0.5,
        "duration_seconds": 10,
        "cost_usd": 0.01,
        "created_at": time.time(),
    }
    payload.update(overrides)
    return event_store.append_event(payload)


def test_deterministic_source_clusters_agent_system_events_and_env_thresholds(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_DEV_SIGNAL_STATUS_MIN_COUNT", "3")
    event_store = _event_store(tmp_path)
    _append_signal(event_store)
    _append_signal(event_store)

    result = DeterministicSignalSource(event_store, thresholds=default_thresholds()).fetch_clusters(SignalWindow.last_days(7))
    assert result["clusters"] == []

    _append_signal(event_store)
    result = DeterministicSignalSource(event_store, thresholds=default_thresholds()).fetch_clusters(SignalWindow.last_days(7))
    keys = {cluster["key"] for cluster in result["clusters"]}
    assert "terminal_status:failed" in keys
    assert all(cluster["evidence_refs"] for cluster in result["clusters"])


def test_report_generation_persists_distinct_empty_clustered_and_failed_states(tmp_path, monkeypatch):
    signal_store = DevProductionSignalStore(tmp_path / "state.db")
    empty_events = _event_store(tmp_path / "empty")
    empty_report = generate_signal_report(signal_store=signal_store, event_store=empty_events, window_days=7)
    assert empty_report["status"] == "completed_empty"

    event_store = _event_store(tmp_path / "clustered")
    _append_signal(event_store)
    _append_signal(event_store)
    _append_signal(event_store)
    report = generate_signal_report(signal_store=signal_store, event_store=event_store, window_days=7)
    assert report["status"] == "completed_with_clusters"
    assert report["counts"]["proposal_count"] == 3
    assert signal_store.get_report(report["report_id"])["counts"]["proposal_count"] == 3
    assert signal_store.list_proposals()[0]["payload"]["source"] == "production_signal"

    class FailingSource:
        def fetch_clusters(self, window, filters=None):
            raise RuntimeError("boom")

    monkeypatch.setattr(production_signals, "_source_impl", lambda *args, **kwargs: FailingSource())
    failed = generate_signal_report(signal_store=signal_store, event_store=event_store, window_days=7)
    assert failed["status"] == "analysis_failed"
    assert failed["warnings"]


def test_multi_source_digest_reports_partial_failure_and_reuses_proposals(tmp_path, monkeypatch):
    signal_store = DevProductionSignalStore(tmp_path / "state.db")
    event_store = _event_store(tmp_path)
    _append_signal(event_store)
    _append_signal(event_store)
    _append_signal(event_store)

    original_source_impl = production_signals._source_impl

    class FailingSource:
        def fetch_clusters(self, window, filters=None):
            raise RuntimeError("source down")

    def source_impl(source, **kwargs):
        if source == "broken":
            return FailingSource()
        return original_source_impl(source, **kwargs)

    monkeypatch.setattr(production_signals, "_source_impl", source_impl)
    first = run_signal_digest_sources(
        signal_store=signal_store,
        event_store=event_store,
        sources=["deterministic", "broken"],
        window_days=7,
    )
    proposal_count = len(signal_store.list_proposals(limit=50))
    second = run_signal_digest_sources(
        signal_store=signal_store,
        event_store=event_store,
        sources=["deterministic"],
        window_days=7,
    )

    assert first["ok"] is False
    assert first["status"] == "partial_source_failure"
    assert first["summary"]["failed_source_count"] == 1
    assert second["ok"] is True
    assert len(signal_store.list_proposals(limit=50)) == proposal_count


def test_signal_digest_runner_lock_skip_is_advisory_success(tmp_path):
    db_path = tmp_path / "state.db"
    lock_path = tmp_path / "digest.lock"
    lock_path.write_text("held")
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[2] / "scripts" / "run_dev_signal_digest.py"),
            "--db-path",
            str(db_path),
            "--lock-path",
            str(lock_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout.strip())
    assert completed.returncode == 0
    assert payload["ok"] is True
    assert payload["status"] == "skipped_lock"


def test_signal_health_exposes_never_ran_status(tmp_path):
    signal_store = DevProductionSignalStore(tmp_path / "state.db")
    health = signal_health(signal_store=signal_store, event_store=None)
    assert health["status"] == "never_ran"
    assert health["latest_report"] is None


def test_front_half_dev_data_can_still_have_empty_self_improvement_loop(tmp_path):
    db_path = tmp_path / "state.db"
    event_store = SubagentEventStore(db_path)
    _append_signal(event_store)
    signal_store = DevProductionSignalStore(db_path)
    verification_store = DevVerificationStore(db_path)
    reliability_store = DevReliabilityStore(db_path)
    lab_store = DevLabLoopStore(db_path)

    assert event_store.list_events(limit=10)
    assert signal_store.list_reports(limit=10) == []
    assert signal_store.list_proposals(limit=10) == []
    assert verification_store.list_runs(limit=10) == []
    assert reliability_store.list_outcomes(limit=10) == []
    assert lab_store.list_passes(limit=10) == []
    assert signal_health(signal_store=signal_store, event_store=event_store)["status"] == "never_ran"
    lab_health = loop_health(db_path=db_path)
    assert lab_health["state"]["status"] == "idle"
    assert lab_health["recent_passes"] == []


def test_laminar_sql_source_uses_parameters_and_fails_open(monkeypatch):
    calls = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps({
                "data": [{
                    "key": "terminal_status:failed",
                    "title": "Repeated failures",
                    "count": 2,
                    "evidence_refs": ["trace-1"],
                    "sample_summaries": ["2 failed"],
                }]
            }).encode("utf-8")

    def fake_urlopen(request, timeout):
        calls["url"] = request.full_url
        calls["timeout"] = timeout
        calls["body"] = json.loads(request.data.decode("utf-8"))
        calls["auth"] = request.headers.get("Authorization")
        return Response()

    monkeypatch.setattr("gateway.dev_control.signal_source.urllib.request.urlopen", fake_urlopen)
    source = LaminarSignalSource(base_url="http://laminar.local", api_key="secret", timeout_seconds=3)
    result = source.fetch_clusters(SignalWindow(start=1, end=2), filters={"domain": "agent-system"})

    assert calls["url"] == "http://laminar.local/v1/sql/query"
    assert calls["body"]["parameters"]["start_time"] == 1
    assert calls["body"]["parameters"]["domain"] == "agent-system"
    assert "start_time" in calls["body"]["query"]
    assert result["clusters"][0]["key"] == "terminal_status:failed"

    def failing_urlopen(request, timeout):
        raise RuntimeError("ClickHouse read-only client is not configured")

    monkeypatch.setattr("gateway.dev_control.signal_source.urllib.request.urlopen", failing_urlopen)
    failed = source.fetch_clusters(SignalWindow(start=1, end=2))
    assert failed["clusters"] == []
    assert "ClickHouse read-only client is not configured" in failed["warnings"][0]


def test_laminar_exporter_redacts_and_fails_open(monkeypatch):
    event = {
        "event_id": 1,
        "session_id": "session-1",
        "launch_plan_id": "plan-1",
        "launch_task_id": "task-1",
        "runtime": "fixture",
        "status": "failed",
        "verification_verdict": "failed",
        "worker_confidence": 0.2,
        "output_contract_score": 0.4,
        "cost_usd": 1.5,
        "duration_seconds": 30,
        "api_key": "sk-real-secret",
    }
    attrs = trace_attributes_for_event(event)
    redacted = redact_attributes({**attrs, "authorization": "Bearer token", "prompt": "x" * 1000})
    assert redacted["authorization"] == "[REDACTED]"
    assert len(redacted["prompt"]) < 600
    assert attrs["plan_id"] == "plan-1"

    sent = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout):
        sent["body"] = json.loads(request.data.decode("utf-8"))
        return Response()

    monkeypatch.setenv("HERMES_LAMINAR_EXPORT_ENABLED", "1")
    monkeypatch.setattr("gateway.dev_control.laminar_exporter.urllib.request.urlopen", fake_urlopen)
    assert export_subagent_event(event)["status"] == "exported"
    attributes = sent["body"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
    assert any(item["key"] == "plan_id" and item["value"]["stringValue"] == "plan-1" for item in attributes)

    monkeypatch.setattr("gateway.dev_control.laminar_exporter.urllib.request.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("down")))
    assert export_subagent_event(event)["status"] == "failed_open"


def test_proposal_transitions_and_outcome_measurement_persist_before_after(tmp_path, monkeypatch):
    signal_store = DevProductionSignalStore(tmp_path / "state.db")
    event_store = _event_store(tmp_path)
    _append_signal(event_store)
    _append_signal(event_store)
    report = generate_signal_report(signal_store=signal_store, event_store=event_store, window_days=7)
    proposal = report["proposals"][0]

    approved = transition_backlog_proposal(
        signal_store=signal_store,
        clarification_store=None,
        proposal_id=proposal["proposal_id"],
        action="approve",
    )
    assert approved["status"] == "approved"

    class AfterSource:
        def fetch_clusters(self, window, filters=None):
            return {
                "source": "deterministic",
                "clusters": [{
                    "key": proposal["cluster_key"],
                    "count": 1,
                    "evidence_refs": [],
                    "sample_summaries": [],
                    "metrics": {},
                }],
                "warnings": [],
                "analyzed_event_count": 1,
            }

    monkeypatch.setattr(production_signals, "_source_impl", lambda *args, **kwargs: AfterSource())
    measured = measure_proposal_outcome(
        signal_store=signal_store,
        event_store=event_store,
        proposal_id=proposal["proposal_id"],
        window_days=7,
    )
    assert measured["outcome"]["before_rate"] > measured["outcome"]["after_rate"]
    assert measured["outcome"]["status"] == "improved"
    assert signal_store.get_proposal(proposal["proposal_id"])["outcome"]["measured_at"]

    health = signal_health(signal_store=signal_store, event_store=event_store)
    assert health["last_analysis_status"] == "completed_with_clusters"
    assert health["open_proposal_count"] >= 1
