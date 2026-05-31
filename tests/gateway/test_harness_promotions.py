import shutil

from gateway.dev_control.harness_promotions import (
    DevHarnessPromotionStore,
    confirm_stable_promotion,
    create_promotion_candidate,
    evaluate_promotion_qualification,
    generate_promotion_package,
    qualify_promotion,
    record_promotion_merge,
)
from gateway.dev_control.lab_loop import promotion_evidence_from_lab_pass


def _lab_evidence(**overrides):
    base = {
        "environment": "lab",
        "source": "dev_lab_loop_pass",
        "candidate_id": "dogfood:candidate-1",
        "pass_id": "devlab-pass-1",
        "target_repo": "hermes-agent",
        "target_capability": "dev-harness",
        "improvement_category": "output_contract",
        "draft_artifact": {"artifact_id": "draft-1", "branch": "lab/promo", "head_sha": "abc123", "ready": True},
        "diff_scope": {"ok": True},
        "touched_paths": ["gateway/dev_control/worker_output_contract.py"],
        "gate_verdicts": {"verification": "passed", "ci": "success", "review": "approved"},
        "quarantined": False,
        "empty_diff": False,
    }
    base.update(overrides)
    return base


def _benchmark(**overrides):
    base = {
        "benchmark_run_ids": ["bench-before", "bench-after"],
        "benchmark_run_id": "bench-after",
        "comparable": True,
        "live": True,
        "sample_count": 8,
        "baseline": {
            "task_set_hash": "tasks-v1",
            "scoring_rubric": "rubric-v1",
            "runtime_profile": "platform.implement",
            "metrics": {
                "score": 0.72,
                "failure_rate": 0.10,
                "verification_success_rate": 0.90,
                "ci_success_rate": 0.95,
                "review_success_rate": 0.90,
                "cost_usd": 1.0,
                "duration_seconds": 100.0,
            },
        },
        "candidate": {
            "task_set_hash": "tasks-v1",
            "scoring_rubric": "rubric-v1",
            "runtime_profile": "platform.implement",
            "metrics": {
                "score": 0.81,
                "failure_rate": 0.10,
                "verification_success_rate": 0.90,
                "ci_success_rate": 0.95,
                "review_success_rate": 0.90,
                "cost_usd": 1.05,
                "duration_seconds": 104.0,
            },
        },
    }
    base.update(overrides)
    return base


def _promotion_payload(**overrides):
    payload = {
        "candidate_id": "dogfood:candidate-1",
        "lab_pass_id": "devlab-pass-1",
        "target_repo": "hermes-agent",
        "target_capability": "dev-harness",
        "improvement_category": "output_contract",
        "benchmark_run_ids": ["bench-before", "bench-after"],
        "lab_evidence": _lab_evidence(),
        "benchmark_evidence": _benchmark(),
    }
    payload.update(overrides)
    return payload


def test_store_creates_empty_and_existing_db_and_compacts_json_evidence(tmp_path):
    db_path = tmp_path / "state.db"
    store = DevHarnessPromotionStore(db_path)
    existing = store._conn.execute("SELECT name FROM sqlite_master WHERE name = 'dev_harness_promotions'").fetchone()
    assert existing is not None
    promotion = store.create_promotion(_promotion_payload(lab_evidence={**_lab_evidence(), "raw_transcript": "secret"}))
    assert promotion["status"] == "candidate"
    assert promotion["lab_evidence"]["environment"] == "lab"
    assert promotion["stable_evidence"]["environment"] == "stable"
    assert promotion["lab_evidence"]["raw_transcript"] == "<omitted>"
    store.close()

    reopened = DevHarnessPromotionStore(db_path)
    assert reopened.get_promotion(promotion["promotion_id"])["promotion_id"] == promotion["promotion_id"]
    reopened.close()


def test_qualification_marks_clear_comparable_improvement_qualified():
    qualification = evaluate_promotion_qualification(_promotion_payload())
    assert qualification["status"] == "qualified"
    assert qualification["metric_deltas"]["score"]["delta"] > 0.05


def test_qualification_marks_inconclusive_when_benchmark_not_comparable():
    qualification = evaluate_promotion_qualification(_promotion_payload(
        benchmark_evidence=_benchmark(comparable=False),
    ))
    assert qualification["status"] == "inconclusive"
    assert any(blocker["code"] == "benchmark_not_comparable" for blocker in qualification["blockers"])


def test_qualification_blocks_missing_provenance_and_missing_benchmark():
    qualification = evaluate_promotion_qualification(_promotion_payload(
        candidate_id=None,
        lab_pass_id=None,
        target_repo=None,
        benchmark_run_ids=[],
        lab_evidence={},
        benchmark_evidence={},
    ))
    assert qualification["status"] == "blocked"
    fields = {blocker.get("field") for blocker in qualification["blockers"]}
    assert {"candidate_id", "lab_pass_id", "target_repo", "benchmark_evidence", "draft_artifact_or_head_sha"} <= fields


def test_qualification_marks_regressed_guardrail():
    benchmark = _benchmark(candidate={
        **_benchmark()["candidate"],
        "metrics": {**_benchmark()["candidate"]["metrics"], "score": 0.83, "failure_rate": 0.15},
    })
    qualification = evaluate_promotion_qualification(_promotion_payload(benchmark_evidence=benchmark))
    assert qualification["status"] == "regressed"
    assert any(blocker["code"] == "guardrail_regressed" for blocker in qualification["blockers"])


def test_fixture_only_or_dry_run_only_evidence_cannot_qualify():
    for benchmark in (
        _benchmark(live=False, mode="fixture"),
        _benchmark(dry_run_only=True),
    ):
        qualification = evaluate_promotion_qualification(_promotion_payload(benchmark_evidence=benchmark))
        assert qualification["status"] == "blocked"
        assert any(blocker["code"] == "fixture_only_evidence" for blocker in qualification["blockers"])


def test_lab_pass_compact_evidence_handles_valid_and_blocked_cases():
    report = {
        "pass_id": "devlab-pass-2",
        "candidate_id": "dogfood:candidate-2",
        "status": "completed",
        "candidate": {"target_paths": ["gateway/dev_control/"]},
        "execution": {
            "draft_artifact": {"artifact_id": "draft-2", "branch": "lab/ok", "head_sha": "def456", "ready": True},
            "diff_scope": {"ok": True},
            "touched_paths": ["gateway/dev_control/routes.py"],
            "verification": {"verification_run_id": "verify-1"},
            "messages": ["do not copy"],
        },
    }
    evidence = promotion_evidence_from_lab_pass(
        report,
        benchmark_run_ids=["bench-1"],
        target_repo="hermes-agent",
        target_capability="dev-harness",
        improvement_category="output_contract",
    )
    assert evidence["pass_id"] == "devlab-pass-2"
    assert evidence["draft_artifact"]["head_sha"] == "def456"
    assert evidence["benchmark_run_ids"] == ["bench-1"]
    assert "messages" not in evidence

    quarantined = promotion_evidence_from_lab_pass({
        **report,
        "execution": {**report["execution"], "quarantined": True, "diff_scope": {"ok": False, "reason": "out_of_scope"}},
    })
    qualification = evaluate_promotion_qualification(_promotion_payload(lab_evidence=quarantined))
    assert qualification["status"] == "blocked"
    assert any(blocker["code"] == "lab_pass_quarantined" for blocker in qualification["blockers"])


def test_missing_draft_artifact_blocks_valid_promotion_candidate(tmp_path):
    store = DevHarnessPromotionStore(tmp_path / "state.db")
    promotion = create_promotion_candidate(
        store=store,
        lab_evidence=_lab_evidence(draft_artifact={}, branch=None, head_sha=None),
        benchmark_evidence=_benchmark(),
        target_repo="hermes-agent",
        target_capability="dev-harness",
        improvement_category="output_contract",
        benchmark_run_ids=["bench-before", "bench-after"],
        qualify=True,
    )
    assert promotion["status"] == "blocked"
    assert any(blocker.get("field") == "draft_artifact_or_head_sha" for blocker in promotion["blockers"])


def test_package_generation_is_advisory_and_blocks_unqualified(tmp_path):
    store = DevHarnessPromotionStore(tmp_path / "state.db")
    promotion = store.create_promotion(_promotion_payload())
    blocked = generate_promotion_package(store=store, promotion_id=promotion["promotion_id"])
    assert blocked["package"]["blocked"] is True
    assert blocked["package"]["side_effects"]["merged"] is False

    qualified = qualify_promotion(store=store, promotion_id=promotion["promotion_id"])
    assert qualified["status"] == "qualified"
    packaged = generate_promotion_package(store=store, promotion_id=promotion["promotion_id"])
    assert packaged["status"] == "packaged"
    assert packaged["package"]["ok"] is True
    assert packaged["package"]["side_effects"] == {
        "merged": False,
        "released": False,
        "published": False,
        "branch_protection_changed": False,
        "service_mutated": False,
    }
    assert "Stable confirmation has not run" in packaged["package"]["body"]


def test_package_can_reference_lab_experiment_without_stable_confirmation(tmp_path):
    store = DevHarnessPromotionStore(tmp_path / "state.db")
    promotion = store.create_promotion(_promotion_payload(
        benchmark_evidence={
            **_benchmark(),
            "lab_experiment": {
                "experiment_id": "devexp-123",
                "decision_status": "promote",
                "metric_deltas": {"score": {"delta": 0.09}},
                "evidence_refs": [{"run_id": "bench-after", "role": "candidate"}],
            },
        },
    ))
    qualify_promotion(store=store, promotion_id=promotion["promotion_id"])
    packaged = generate_promotion_package(store=store, promotion_id=promotion["promotion_id"])
    assert packaged["package"]["evidence_ids"]["lab_experiment_id"] == "devexp-123"
    assert packaged["package"]["lab_experiment"]["stable_confirmation_required"] is True
    assert "Lab experiment: devexp-123" in packaged["package"]["body"]
    assert packaged["stable_evidence"]["environment"] == "stable"
    assert "confirmation" not in packaged["stable_evidence"]


def test_stable_confirmation_is_separate_from_lab_evidence(tmp_path):
    store = DevHarnessPromotionStore(tmp_path / "state.db")
    promotion = store.create_promotion(_promotion_payload())
    promotion = qualify_promotion(store=store, promotion_id=promotion["promotion_id"])
    promotion = record_promotion_merge(
        store=store,
        promotion_id=promotion["promotion_id"],
        merge_refs={"child_pr": "https://github.test/hermes-agent/pull/1", "child_sha": "abc123"},
    )
    assert promotion["status"] == "merged"
    assert promotion["stable_evidence"]["environment"] == "stable"
    assert "confirmation" not in promotion["stable_evidence"]

    confirmed = confirm_stable_promotion(
        store=store,
        promotion_id=promotion["promotion_id"],
        stable_evidence={"benchmark_run_ids": ["stable-bench-1"], "benchmark_evidence": _benchmark()},
    )
    assert confirmed["status"] == "stable_confirmed"
    assert confirmed["stable_evidence"]["environment"] == "stable"
    assert confirmed["stable_evidence"]["confirmation"]["status"] == "qualified"


def test_copied_db_stable_confirmation_smoke_does_not_mutate_source(tmp_path):
    source_db = tmp_path / "source.db"
    source_store = DevHarnessPromotionStore(source_db)
    promotion = source_store.create_promotion(_promotion_payload())
    promotion = qualify_promotion(store=source_store, promotion_id=promotion["promotion_id"])
    promotion = record_promotion_merge(
        store=source_store,
        promotion_id=promotion["promotion_id"],
        merge_refs={"child_sha": "abc123"},
    )
    source_store.close()

    copied_db = tmp_path / "copy.db"
    shutil.copyfile(source_db, copied_db)
    copy_store = DevHarnessPromotionStore(copied_db)
    confirmed = confirm_stable_promotion(
        store=copy_store,
        promotion_id=promotion["promotion_id"],
        stable_evidence={"benchmark_run_ids": ["stable-bench-1"], "benchmark_evidence": _benchmark()},
    )
    assert confirmed["status"] == "stable_confirmed"
    copy_store.close()

    source_store = DevHarnessPromotionStore(source_db)
    source = source_store.get_promotion(promotion["promotion_id"])
    assert source["status"] == "merged"
    assert "confirmation" not in source["stable_evidence"]
    source_store.close()
