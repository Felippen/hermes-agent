from __future__ import annotations

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

    for forbidden in ("Synapse", "Spine", "Cortex", "Axon", "sqlite", "open("):
        assert forbidden not in source
