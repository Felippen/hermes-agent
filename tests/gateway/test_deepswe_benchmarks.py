from gateway.dev_control.deepswe_benchmarks import (
    DevDeepSWEBenchmarkStore,
    compact_deepswe_evidence,
    diagnose_deepswe_run,
    evaluate_deepswe_comparability,
    generate_deepswe_actions,
    normalize_deepswe_context,
    parse_deepswe_task_results,
    start_deepswe_benchmark,
)
from gateway.dev_control.harness_promotions import (
    DevHarnessPromotionStore,
    confirm_stable_promotion,
    create_promotion_candidate,
    generate_promotion_package,
    qualify_promotion,
    record_promotion_merge,
)
from gateway.dev_control.lab_loop import DevLabLoopStore
from gateway.dev_control.production_signals import DevProductionSignalStore


def _context(**overrides):
    context = {
        "deepswe_repo_url": "https://github.com/datacurve-ai/deep-swe",
        "deepswe_commit": "abc123",
        "pier_version": "0.1.0",
        "agent_adapter": "mini-swe-agent",
        "model_profile": "openai/gpt-5.5",
        "task_ids": ["py-task-1", "go-task-2", "ts-task-3"],
        "resource_limits": {"timeout_seconds": 600, "max_cost_usd": 5.0, "max_tasks": 3},
        "network_policy": {"mode": "agent_allowlist", "allowed_hosts": ["api.openai.com"]},
        "artifact_dir": "/tmp/deepswe-artifacts",
    }
    context.update(overrides)
    return context


def _task_results():
    return [
        {
            "task_id": "py-task-1",
            "status": "passed",
            "verifier_status": "passed",
            "cost_usd": 0.50,
            "duration_seconds": 120,
            "input_tokens": 1000,
            "output_tokens": 300,
            "prompt": "do not persist",
            "trajectory": "do not persist",
        },
        {
            "task_id": "go-task-2",
            "status": "failed",
            "failure_category": "navigation_failure",
            "message": "could not find relevant files",
            "cost_usd": 0.75,
            "duration_seconds": 180,
            "input_tokens": 1200,
            "output_tokens": 450,
        },
        {
            "task_id": "ts-task-3",
            "status": "failed",
            "failure_category": "navigation_failure",
            "message": "no such file in explored path",
            "cost_usd": 0.80,
            "duration_seconds": 170,
            "input_tokens": 1100,
            "output_tokens": 400,
        },
    ]


def test_store_creates_schema_and_persists_compact_run(tmp_path):
    store = DevDeepSWEBenchmarkStore(tmp_path / "state.db")
    run = start_deepswe_benchmark(
        store=store,
        context=_context(),
        task_results=_task_results(),
        mode="fixture",
    )
    assert run["status"] == "completed"
    assert run["summary"]["pass_rate"] == 0.333333
    assert run["pinned_context"]["task_subset_hash"]
    assert "prompt" not in run["task_results"][0]
    assert "trajectory" not in run["task_results"][0]
    store.close()

    reopened = DevDeepSWEBenchmarkStore(tmp_path / "state.db")
    assert reopened.get_run(run["run_id"])["run_id"] == run["run_id"]
    assert reopened.list_runs()[0]["summary"]["task_count"] == 3


def test_incomplete_context_is_blocked_unless_exploratory(tmp_path):
    store = DevDeepSWEBenchmarkStore(tmp_path / "state.db")
    try:
        start_deepswe_benchmark(
            store=store,
            context={"deepswe_commit": "abc123"},
            task_results=[],
        )
    except ValueError as exc:
        assert "missing" in str(exc).lower()
    else:
        raise AssertionError("Expected incomplete DeepSWE context to fail")

    exploratory_context = normalize_deepswe_context({"deepswe_commit": "abc123", "exploratory": True})
    assert exploratory_context["blockers"]
    assert exploratory_context["exploratory"] is True


def test_compact_evidence_omits_benchmark_data():
    compact = compact_deepswe_evidence({
        "prompt": "secret prompt",
        "reference_solution": "secret patch",
        "verifier_patch": "secret tests",
        "trajectory": "secret rollout",
        "summary": {"pass_rate": 0.5},
    })
    assert compact["prompt"] == "<omitted>"
    assert compact["reference_solution"] == "<omitted>"
    assert compact["verifier_patch"] == "<omitted>"
    assert compact["trajectory"] == "<omitted>"
    assert compact["summary"]["pass_rate"] == 0.5


def test_parser_and_summary_classify_infrastructure_without_model_failure(tmp_path):
    store = DevDeepSWEBenchmarkStore(tmp_path / "state.db")
    run = start_deepswe_benchmark(
        store=store,
        context=_context(task_ids=["a", "b", "c", "d", "e"]),
        task_results=[
            {"task_id": "task-1", "status": "error", "message": "missing image dependency"},
        ],
    )
    assert run["summary"]["infrastructure_failure_rate"] == 1.0
    assert run["task_results"][0]["failure_category"] == "dependency_environment_failure"


def test_deepswe_comparability_detects_good_and_bad_runs(tmp_path):
    store = DevDeepSWEBenchmarkStore(tmp_path / "state.db")
    baseline = start_deepswe_benchmark(
        store=store,
        context=_context(),
        task_results=[{"task_id": "a", "status": "passed"}, {"task_id": "b", "status": "failed"}],
    )
    candidate = start_deepswe_benchmark(
        store=store,
        context=_context(),
        task_results=[{"task_id": "a", "status": "passed"}, {"task_id": "b", "status": "passed"}],
    )
    comparable = evaluate_deepswe_comparability(baseline, candidate)
    assert comparable["status"] == "qualified"
    assert comparable["metric_deltas"]["pass_rate"]["delta"] == 0.5

    changed = start_deepswe_benchmark(
        store=store,
        context=_context(deepswe_commit="different"),
        task_results=[{"task_id": "a", "status": "passed"}, {"task_id": "b", "status": "passed"}],
    )
    inconclusive = evaluate_deepswe_comparability(baseline, changed)
    assert inconclusive["status"] == "inconclusive"
    assert any(blocker["code"] == "deepswe_not_comparable" for blocker in inconclusive["blockers"])


def test_diagnosis_and_actions_are_advisory_and_do_not_auto_approve(tmp_path):
    db_path = tmp_path / "state.db"
    store = DevDeepSWEBenchmarkStore(db_path)
    signal_store = DevProductionSignalStore(db_path)
    lab_store = DevLabLoopStore(db_path)
    run = start_deepswe_benchmark(store=store, context=_context(), task_results=_task_results())

    diagnosis = diagnose_deepswe_run(store=store, run_id=run["run_id"])
    assert diagnosis["exploratory"] is False
    assert diagnosis["patterns"][0]["actionable"] is True
    assert diagnosis["patterns"][0]["failure_category"] == "navigation_failure"

    generated = generate_deepswe_actions(
        store=store,
        diagnosis_id=diagnosis["diagnosis_id"],
        signal_store=signal_store,
        lab_store=lab_store,
        create_backlog_proposals=True,
        create_lab_candidates=True,
    )
    assert generated["created_count"] == 1
    action = generated["created"][0]
    assert action["proposal_id"]
    assert action["lab_candidate_id"]
    candidate = lab_store.get_candidate(action["lab_candidate_id"])
    assert candidate["approved"] is False


def test_weak_single_sample_diagnosis_stays_exploratory(tmp_path):
    store = DevDeepSWEBenchmarkStore(tmp_path / "state.db")
    run = start_deepswe_benchmark(
        store=store,
        context=_context(task_ids=["one"]),
        task_results=[{"task_id": "one", "status": "failed", "failure_category": "verifier_failure"}],
    )
    diagnosis = diagnose_deepswe_run(store=store, run_id=run["run_id"])
    generated = generate_deepswe_actions(store=store, diagnosis_id=diagnosis["diagnosis_id"])
    assert diagnosis["exploratory"] is True
    assert generated["created_count"] == 0


def test_deepswe_evidence_supports_promotion_but_not_stable_confirmation(tmp_path):
    db_path = tmp_path / "state.db"
    deepswe_store = DevDeepSWEBenchmarkStore(db_path)
    promotion_store = DevHarnessPromotionStore(db_path)
    baseline = start_deepswe_benchmark(
        store=deepswe_store,
        context=_context(task_ids=["a", "b", "c", "d", "e"], resource_limits={"timeout_seconds": 600, "max_cost_usd": 5.0, "max_tasks": 5}),
        task_results=[
            {"task_id": "a", "status": "passed"},
            {"task_id": "b", "status": "failed", "failure_category": "navigation_failure"},
            {"task_id": "c", "status": "failed", "failure_category": "navigation_failure"},
            {"task_id": "d", "status": "failed", "failure_category": "verifier_failure"},
            {"task_id": "e", "status": "failed", "failure_category": "verifier_failure"},
        ],
    )
    candidate = start_deepswe_benchmark(
        store=deepswe_store,
        context=_context(task_ids=["a", "b", "c", "d", "e"], resource_limits={"timeout_seconds": 600, "max_cost_usd": 5.0, "max_tasks": 5}),
        task_results=[
            {"task_id": "a", "status": "passed"},
            {"task_id": "b", "status": "passed"},
            {"task_id": "c", "status": "passed"},
            {"task_id": "d", "status": "passed"},
            {"task_id": "e", "status": "failed", "failure_category": "verifier_failure"},
        ],
    )
    benchmark_evidence = {
        "provider": "deepswe",
        "deepswe_run_ids": [baseline["run_id"], candidate["run_id"]],
        "has_live_evidence": True,
        "baseline": {
            "run_id": baseline["run_id"],
            "pinned_context": baseline["pinned_context"],
            "summary": baseline["summary"],
        },
        "candidate": {
            "run_id": candidate["run_id"],
            "pinned_context": candidate["pinned_context"],
            "summary": candidate["summary"],
        },
        "action_ids": ["devdswe-act-1"],
        "failure_patterns": [{"failure_category": "navigation_failure"}],
    }
    promotion = create_promotion_candidate(
        store=promotion_store,
        lab_evidence={
            "candidate_id": "dogfood:deepswe",
            "pass_id": "devlab-pass-deepswe",
            "target_repo": "hermes-agent",
            "target_capability": "dev-harness",
            "draft_artifact": {"head_sha": "abc123"},
            "diff_scope": {"ok": True},
            "quarantined": False,
            "empty_diff": False,
        },
        benchmark_evidence=benchmark_evidence,
        target_repo="hermes-agent",
        target_capability="dev-harness",
        improvement_category="output_contract",
        benchmark_run_ids=[baseline["run_id"], candidate["run_id"]],
        qualify=True,
    )
    assert promotion["status"] == "qualified"
    packaged = generate_promotion_package(store=promotion_store, promotion_id=promotion["promotion_id"])
    assert packaged["package"]["deepswe_evidence"]["task_subset_hash"]
    assert "Raw DeepSWE prompts" in packaged["package"]["body"]
    merged = record_promotion_merge(
        store=promotion_store,
        promotion_id=promotion["promotion_id"],
        merge_refs={"child_sha": "abc123"},
    )
    assert merged["status"] == "merged"
    assert "confirmation" not in merged["stable_evidence"]
    confirmed = confirm_stable_promotion(
        store=promotion_store,
        promotion_id=promotion["promotion_id"],
        stable_evidence={"benchmark_run_ids": ["stable-dswe"], "benchmark_evidence": benchmark_evidence},
    )
    assert confirmed["status"] == "stable_confirmed"
