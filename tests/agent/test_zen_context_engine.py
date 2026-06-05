from __future__ import annotations

import copy
import json

from plugins.context_engine.zen import ZenContextEngine


def test_zen_compiles_source_backed_request_copy_brief() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    messages = [
        {"role": "user", "content": "We must preserve prompt cache behavior."},
        {"role": "assistant", "content": "Plan: inspect run_agent.py and then patch tests."},
        {"role": "tool", "content": "pytest tests/agent/test_context_engine.py passed"},
        {"role": "user", "content": "Now implement the narrow hook in agent/conversation_loop.py"},
    ]

    brief = engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=3,
        model="test",
        platform="cli",
        system_prompt_chars=100,
    )

    assert brief
    assert "Hermes Zen working brief" in brief
    assert "constraint:" in brief
    assert "open_item:" in brief
    assert "source: turn:" in brief
    assert engine.zen_notes
    assert engine.zen_source_pointers


def test_zen_keeps_notes_in_memory_and_clears_on_reset_and_end() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    engine.compile_turn_context(
        session_id="s1",
        user_message="Fix failing tests",
        conversation_history=[{"role": "tool", "content": "command failed with exit code 1"}],
        current_turn_user_idx=0,
    )

    assert engine.zen_notes
    engine.on_session_reset()
    assert engine.zen_notes == ()
    assert engine.zen_source_pointers == {}

    engine.compile_turn_context(
        session_id="s1",
        user_message="Fix failing tests",
        conversation_history=[{"role": "tool", "content": "command failed with exit code 1"}],
        current_turn_user_idx=0,
    )
    assert engine.zen_notes
    engine.on_session_end("s1", [])
    assert engine.zen_notes == ()
    assert engine.zen_source_pointers == {}


def test_zen_exposes_no_model_callable_tools() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)

    assert engine.get_tool_schemas() == []
    result = json.loads(engine.handle_tool_call("zen_expand", {}))
    assert result["error"] == "Hermes Zen v1 exposes no model-callable tools"


def test_zen_context_engine_is_discoverable_by_name() -> None:
    from plugins.context_engine import load_context_engine

    engine = load_context_engine("zen")

    assert isinstance(engine, ZenContextEngine)
    assert engine.name == "zen"


def test_zen_preserves_builtin_compression_surface() -> None:
    from agent.context_compressor import ContextCompressor

    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)

    assert isinstance(engine, ContextCompressor)
    assert callable(engine.compress)
    assert callable(engine.should_compress)


def test_compression_helper_still_notifies_memory_lifecycle_hooks() -> None:
    import inspect
    from agent import conversation_compression

    source = inspect.getsource(conversation_compression.compress_context)

    assert "agent._memory_manager.on_pre_compress(messages)" in source
    assert "agent._memory_manager.on_session_switch(" in source
    assert "reason=\"compression\"" in source


def test_zen_core_does_not_reference_axon_synapse_or_persistent_storage() -> None:
    import inspect
    import plugins.context_engine.zen as zen_module

    source = inspect.getsource(zen_module)

    for forbidden in (
        "Synapse",
        "Spine",
        "Cortex",
        "Axon",
        "sqlite",
        "open(",
        "zen_expand",
        "zen_search_notes",
        "zen_pin_context",
        "Meta-Thinker",
    ):
        assert forbidden not in source


def test_large_tool_observation_is_masked_with_bounded_source_backed_summary() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    noisy_payload = "pytest output line with useful pass signal\n" + ("detail line " * 260)
    messages = [
        {"role": "user", "content": "Summarize the test status."},
        {"role": "tool", "content": noisy_payload},
        {"role": "user", "content": "Use the latest observation."},
    ]

    brief = engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=2,
    )

    masked = [note for note in engine.zen_notes if note.masking is not None]
    assert masked
    note = masked[0]
    assert note.masking.observation_class == "large"
    assert note.masking.original_chars == len(noisy_payload)
    assert note.masking.summary_chars <= 280
    assert noisy_payload not in note.summary
    assert note.source.pointer_id in brief


def test_repeated_tool_observation_dedupes_with_source_coverage() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    repeated = "same status block: all generated files are unchanged"
    messages = [
        {"role": "tool", "content": repeated},
        {"role": "assistant", "content": "Plan: check the repeated output."},
        {"role": "tool", "content": repeated},
        {"role": "user", "content": "Continue from the latest result."},
    ]

    brief = engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=3,
    )

    repeated_notes = [
        note for note in engine.zen_notes
        if note.masking is not None and note.masking.observation_class == "repeated"
    ]
    assert len(repeated_notes) == 1
    note = repeated_notes[0]
    assert note.masking.occurrence_count == 2
    assert len(note.masking.source_pointer_ids) == 2
    assert "Repeated tool observation" in note.summary
    assert "sources:" in brief


def test_failed_tool_observation_becomes_failed_path_not_evidence() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    failure = "command failed with exit code 1: missing dependency in build.sh"

    engine.compile_turn_context(
        session_id="s1",
        user_message="Fix it without repeating the same command.",
        conversation_history=[
            {"role": "tool", "content": failure},
            {"role": "user", "content": "Fix it without repeating the same command."},
        ],
        current_turn_user_idx=1,
    )

    failed = [note for note in engine.zen_notes if note.kind == "failed_path"]
    evidence = [note for note in engine.zen_notes if note.kind == "evidence"]
    assert failed
    assert all(note.masking is None or note.masking.observation_class != "failed" for note in evidence)
    assert "avoid repeating" in failed[0].summary


def test_stale_observation_is_marked_uncertain_in_brief() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    messages = [{"role": "tool", "content": "old observation says retry the previous approach"}]
    messages.extend({"role": "assistant", "content": f"intermediate turn {idx}"} for idx in range(13))
    messages.append({"role": "user", "content": "Use the newer correction instead."})

    brief = engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=len(messages) - 1,
    )

    stale_notes = [note for note in engine.zen_notes if note.masking and note.masking.stale]
    assert stale_notes
    assert "stale/uncertain" in brief


def test_masking_does_not_mutate_canonical_messages() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    messages = [
        {"role": "tool", "content": "large output " * 200},
        {"role": "user", "content": "Keep the original transcript intact."},
    ]
    before = copy.deepcopy(messages)

    engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=1,
    )

    assert messages == before


def test_masking_failure_degrades_without_raising(monkeypatch) -> None:
    import plugins.context_engine.zen as zen_module

    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    monkeypatch.setattr(
        zen_module,
        "_classify_observation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    brief = engine.compile_turn_context(
        session_id="s1",
        user_message="Continue.",
        conversation_history=[
            {"role": "tool", "content": "large output " * 200},
            {"role": "user", "content": "Continue."},
        ],
        current_turn_user_idx=1,
    )

    assert brief
    assert not [note for note in engine.zen_notes if note.masking is not None]


def test_zen_decision_trace_snapshot_is_stable_and_privacy_safe() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    payload = "SENSITIVE_PAYLOAD_VALUE " * 120
    messages = [
        {"role": "tool", "content": payload},
        {"role": "user", "content": "Use the source-backed summary only."},
    ]

    engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=1,
    )

    traces = [trace.to_safe_dict() for trace in engine.zen_decision_traces]
    assert traces
    assert set(traces[0]) == {
        "action",
        "reason_code",
        "confidence",
        "input_source",
        "token_estimate",
        "safety_policy",
        "budget_impact",
        "redacted",
    }
    assert any(trace["action"] == "masked" for trace in traces)
    assert all(trace["redacted"] is True for trace in traces)
    assert all(payload not in json.dumps(trace, sort_keys=True) for trace in traces)
    assert engine.zen_metrics["trace_complete"] is True
    assert engine.zen_metrics["privacy_safe"] is True


def test_zen_metrics_track_context_shape_and_clear_on_reset_and_end() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)
    messages = [
        {"role": "tool", "content": "large output " * 180},
        {"role": "user", "content": "Continue with bounded context."},
    ]

    brief = engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=1,
    )

    metrics = engine.zen_metrics
    assert brief
    assert metrics["context_chars_in"] > metrics["context_chars_out"] > 0
    assert metrics["masked_span_count"] == 1
    assert metrics["masked_count"] >= 1
    assert metrics["selected_count"] >= 1
    assert metrics["compression_ratio"] < 1
    assert metrics["trace_event_count"] == len(engine.zen_decision_traces)

    engine.on_session_reset()
    assert engine.zen_metrics["trace_event_count"] == 0
    assert engine.zen_decision_traces == ()

    engine.compile_turn_context(
        session_id="s1",
        user_message=messages[-1]["content"],
        conversation_history=messages,
        current_turn_user_idx=1,
    )
    assert engine.zen_decision_traces
    engine.on_session_end("s1", messages)
    assert engine.zen_metrics["trace_event_count"] == 0
    assert engine.zen_decision_traces == ()


def test_zen_operator_controls_force_fallback_and_session_bypass() -> None:
    fallback = ZenContextEngine(
        model="test",
        quiet_mode=True,
        config_context_length=200000,
        zen_config={"force_compressor_fallback": True},
    )

    assert fallback.compile_turn_context(
        session_id="s1",
        user_message="Continue.",
        conversation_history=[{"role": "tool", "content": "large output " * 200}],
        current_turn_user_idx=0,
    ) is None
    assert fallback.zen_decision_traces[-1].action == "fallback"
    assert fallback.zen_decision_traces[-1].reason_code == "compressor_fallback_forced"
    assert fallback.zen_metrics["context_chars_out"] == 0

    bypass = ZenContextEngine(
        model="test",
        quiet_mode=True,
        config_context_length=200000,
        zen_config={"bypass_session_ids": {"s2"}},
    )
    assert bypass.compile_turn_context(
        session_id="s2",
        user_message="Continue.",
        conversation_history=[{"role": "tool", "content": "large output " * 200}],
        current_turn_user_idx=0,
    ) is None
    assert bypass.zen_decision_traces[-1].action == "bypassed"
    assert bypass.zen_decision_traces[-1].reason_code == "session_bypassed"


def test_zen_config_invalid_values_fall_back_and_strictness_changes_threshold() -> None:
    relaxed = ZenContextEngine(
        model="test",
        quiet_mode=True,
        config_context_length=200000,
        zen_config={"masking_strictness": "relaxed"},
    )
    strict = ZenContextEngine(
        model="test",
        quiet_mode=True,
        config_context_length=200000,
        zen_config={"masking_strictness": "strict", "trace_verbosity": "unsupported"},
    )
    invalid = ZenContextEngine(
        model="test",
        quiet_mode=True,
        config_context_length=200000,
        zen_config={"masking_strictness": "unsupported"},
    )
    payload = "threshold candidate " * 60
    messages = [
        {"role": "tool", "content": payload},
        {"role": "user", "content": "Continue."},
    ]

    relaxed.compile_turn_context(
        session_id="s1",
        user_message="Continue.",
        conversation_history=messages,
        current_turn_user_idx=1,
    )
    strict.compile_turn_context(
        session_id="s1",
        user_message="Continue.",
        conversation_history=messages,
        current_turn_user_idx=1,
    )
    invalid.compile_turn_context(
        session_id="s1",
        user_message="Continue.",
        conversation_history=messages,
        current_turn_user_idx=1,
    )

    assert not [note for note in relaxed.zen_notes if note.masking is not None]
    assert [note for note in strict.zen_notes if note.masking is not None]
    assert not [note for note in invalid.zen_notes if note.masking is not None]


def test_zen_configure_hook_applies_host_controls() -> None:
    engine = ZenContextEngine(model="test", quiet_mode=True, config_context_length=200000)

    engine.configure_zen({"enabled": False})

    assert engine.compile_turn_context(
        session_id="s1",
        user_message="Continue.",
        conversation_history=[{"role": "tool", "content": "large output " * 200}],
        current_turn_user_idx=0,
    ) is None
    assert engine.zen_decision_traces[-1].reason_code == "zen_disabled"
