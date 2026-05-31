from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.dev_control.lab_experiments import (
    DevLabExperimentStore,
    attach_experiment_evidence,
    evaluate_lab_experiment,
    evaluate_experiment_decision,
    experiment_promotion_reference,
    generate_experiment_actions,
)
from gateway.dev_control.lab_loop import DevLabLoopStore
from gateway.dev_control.production_signals import DevProductionSignalStore
from gateway.dev_control.routes import DevControlRouteMixin, dev_control_capabilities, register_dev_control_routes


def _evidence(role, score, **overrides):
    evidence = {
        "role": role,
        "source": "deepswe",
        "run_id": f"{role}-run",
        "summary": {
            "score": score,
            "sample_count": 5,
            "infrastructure_failure_rate": 0.0,
            "cost_usd": 1.0,
        },
        "context": {
            "benchmark_commit": "abc123",
            "task_subset_hash": "tasks-v1",
            "pier_version": "0.2.0",
            "agent_adapter": "codex",
            "model_profile": "gpt-5.5",
            "resource_limits": {"timeout_seconds": 600, "max_tasks": 5},
            "network_policy": {"mode": "agent_allowlist"},
            "scoring_rubric": "verifier-pass-rate",
        },
        "guardrails": {"verification": "passed", "ci": "success", "review": "approved"},
        "prompt": "do not persist",
        "trajectory": "do not persist",
        "reference_solution": "do not persist",
    }
    evidence.update(overrides)
    return evidence


def test_store_creates_schema_persists_and_compacts_experiment(tmp_path):
    store = DevLabExperimentStore(tmp_path / "state.db")
    experiment = store.create_experiment({
        "hypothesis": "Codex egress fix improves DeepSWE pass rate",
        "target_area": "dev_harness",
        "owner": "lab",
        "scope": {"prompt": "secret", "target_paths": ["gateway/dev_control/"]},
    })
    assert experiment["status"] == "draft"
    assert experiment["scope"]["prompt"] == "<omitted>"
    store.close()

    reopened = DevLabExperimentStore(tmp_path / "state.db")
    assert reopened.get_experiment(experiment["experiment_id"])["hypothesis"].startswith("Codex")
    assert reopened.list_experiments(target_area="dev_harness")[0]["experiment_id"] == experiment["experiment_id"]
    reopened.close()


def test_attach_evidence_compacts_raw_benchmark_data_and_missing_refs_block(tmp_path):
    store = DevLabExperimentStore(tmp_path / "state.db")
    experiment = store.create_experiment({"hypothesis": "Measure candidate", "target_area": "dev_harness"})
    updated = attach_experiment_evidence(
        store=store,
        experiment_id=experiment["experiment_id"],
        evidence=_evidence("candidate", 0.8, run_id="missing-run"),
        stores={"deepswe": SimpleNamespace(get_run=lambda _run_id: None)},
    )
    ref = updated["evidence_refs"][0]
    assert ref["prompt"] == "<omitted>" if "prompt" in ref else True
    assert ref["validation_status"] == "missing"
    assert any(blocker["code"] == "deepswe_run_missing" for blocker in updated["blockers"])
    assert "trajectory" not in ref


def test_evaluation_promotes_comparable_improvement_and_requires_stable_confirmation(tmp_path):
    store = DevLabExperimentStore(tmp_path / "state.db")
    experiment = store.create_experiment({"hypothesis": "Candidate improves score", "target_area": "dev_harness"})
    experiment = attach_experiment_evidence(
        store=store,
        experiment_id=experiment["experiment_id"],
        evidence=[_evidence("baseline", 0.70), _evidence("candidate", 0.82)],
    )
    evaluated = evaluate_lab_experiment(store=store, experiment_id=experiment["experiment_id"])
    assert evaluated["status"] == "promote"
    assert evaluated["decision"]["promotable"] is True
    assert evaluated["decision"]["stable_confirmation_required"] is True
    assert evaluated["decision"]["side_effects"]["merged"] is False

    ref = experiment_promotion_reference(evaluated)
    assert ref["decision_status"] == "promote"
    assert ref["stable_confirmation_required"] is True


def test_evaluation_blocks_non_comparable_infrastructure_noise_missing_guardrails_and_small_samples():
    baseline = _evidence("baseline", 0.70)
    candidate = _evidence(
        "candidate",
        0.90,
        summary={"score": 0.90, "sample_count": 1, "infrastructure_failure_rate": 0.50},
        context={**baseline["context"], "model_profile": "different"},
        guardrails={},
    )
    decision = evaluate_experiment_decision({"experiment_id": "exp", "evidence_refs": [baseline, candidate]})
    codes = {blocker["code"] for blocker in decision["blockers"]}
    assert decision["status"] == "inconclusive"
    assert {"experiment_not_comparable", "sample_count_below_threshold", "infrastructure_noise", "guardrails_missing"} <= codes


def test_evaluation_rejects_regression_and_iterates_on_failure_patterns():
    reject = evaluate_experiment_decision({
        "experiment_id": "exp-reject",
        "evidence_refs": [_evidence("baseline", 0.80), _evidence("candidate", 0.70)],
    })
    assert reject["status"] == "reject"

    iterate = evaluate_experiment_decision({
        "experiment_id": "exp-iterate",
        "evidence_refs": [
            _evidence("baseline", 0.80),
            _evidence("candidate", 0.80, failure_patterns=[{"failure_category": "navigation_failure", "severity": "high"}]),
        ],
    })
    assert iterate["status"] == "iterate"


def test_action_generation_is_advisory_and_unapproved(tmp_path):
    db_path = tmp_path / "state.db"
    store = DevLabExperimentStore(db_path)
    signal_store = DevProductionSignalStore(db_path)
    lab_store = DevLabLoopStore(db_path)
    experiment = store.create_experiment({"hypothesis": "Needs iteration", "target_area": "dev_harness"})
    experiment = attach_experiment_evidence(
        store=store,
        experiment_id=experiment["experiment_id"],
        evidence=[
            _evidence("baseline", 0.80),
            _evidence("candidate", 0.80, failure_patterns=[{"failure_category": "wrong_file_edit", "severity": "high"}]),
        ],
    )
    evaluate_lab_experiment(store=store, experiment_id=experiment["experiment_id"])
    actions = generate_experiment_actions(
        store=store,
        experiment_id=experiment["experiment_id"],
        signal_store=signal_store,
        lab_store=lab_store,
        create_backlog_proposals=True,
        create_lab_candidates=True,
    )
    assert actions[0]["proposal_id"]
    candidate = lab_store.get_candidate(actions[0]["lab_candidate_id"])
    assert candidate["approved"] is False
    assert candidate["source"] == "lab_experiment"


class _RouteAdapter(DevControlRouteMixin):
    def __init__(self, db_path):
        self._execution_store = SimpleNamespace(db_path=db_path)

    def _check_auth(self, _request):
        return None

    def _ensure_dev_execution_store(self):
        return self._execution_store


@pytest.mark.asyncio
async def test_lab_experiment_routes_and_capabilities(tmp_path):
    assert dev_control_capabilities()["dev_lab_experiments"]["path"] == "/v1/dev/lab/experiments"
    app = web.Application()
    adapter = _RouteAdapter(tmp_path / "state.db")
    register_dev_control_routes(app, adapter)

    async with TestClient(TestServer(app)) as client:
        created = await client.post("/v1/dev/lab/experiments", json={"hypothesis": "Route experiment", "target_area": "dev_harness"})
        assert created.status == 200
        experiment = await created.json()

        evidence = await client.post(
            f"/v1/dev/lab/experiments/{experiment['experiment_id']}/evidence",
            json={"evidence": [_evidence("baseline", 0.70), _evidence("candidate", 0.82)]},
        )
        assert evidence.status == 200

        evaluated = await client.post(f"/v1/dev/lab/experiments/{experiment['experiment_id']}/evaluate", json={})
        assert evaluated.status == 200
        assert (await evaluated.json())["status"] == "promote"

        listed = await client.get("/v1/dev/lab/experiments")
        payload = await listed.json()
        assert payload["total"] == 1
