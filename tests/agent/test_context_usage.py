from types import SimpleNamespace
from unittest.mock import patch

from agent.context_usage import build_context_usage_payload, emit_context_usage


class _FakeCompressor:
    last_prompt_tokens = 111_400
    context_length = 200_000
    compression_count = 2


def test_build_context_usage_payload_includes_context_and_session_fields():
    agent = SimpleNamespace(
        model="openai/gpt-5.5-pro",
        session_input_tokens=50_000,
        session_output_tokens=20_000,
        session_cache_read_tokens=5_000,
        session_cache_write_tokens=1_000,
        session_reasoning_tokens=500,
        session_prompt_tokens=50_000,
        session_completion_tokens=20_000,
        session_total_tokens=70_000,
        context_compressor=_FakeCompressor(),
        provider="openrouter",
        base_url="https://openrouter.ai/api/v1",
    )

    payload = build_context_usage_payload(agent)

    assert payload["model"] == "openai/gpt-5.5-pro"
    assert payload["context_used"] == 111_400
    assert payload["context_max"] == 200_000
    assert payload["context_percent"] == 56
    assert payload["compressions"] == 2
    assert payload["session"]["input_tokens"] == 50_000
    assert payload["session"]["output_tokens"] == 20_000
    assert payload["session"]["cache_read_tokens"] == 5_000
    assert payload["session"]["reasoning_tokens"] == 500


def test_build_context_usage_payload_emits_categories_from_prompt_parts_and_tools():
    agent = SimpleNamespace(
        model="openai/gpt-5.5-pro",
        session_input_tokens=0,
        session_output_tokens=0,
        session_cache_read_tokens=0,
        session_cache_write_tokens=0,
        session_reasoning_tokens=0,
        session_prompt_tokens=0,
        session_completion_tokens=0,
        session_total_tokens=0,
        context_compressor=_FakeCompressor(),
        provider="openrouter",
        base_url="",
        tools=[{"type": "function", "function": {"name": "shell"}}],
        conversation_history=[
            {"role": "user", "content": "x" * 400},
            {"role": "assistant", "content": "y" * 200},
        ],
    )

    fake_parts = {
        "stable": "a" * 800,  # ~200 tokens
        "context": "b" * 1600,  # ~400 tokens
        "volatile": "c" * 200,  # ~50 tokens
    }

    with patch(
        "agent.system_prompt.build_system_prompt_parts",
        return_value=fake_parts,
    ):
        payload = build_context_usage_payload(agent)

    categories = payload.get("categories")
    assert categories, "expected a categories array in the payload"
    by_key = {entry["key"]: entry for entry in categories}
    # Overhead estimate (~665 tokens) fits well inside the measured 111_400, so
    # the overhead buckets are kept as-is and the slack becomes Conversation.
    assert by_key["system"]["label"] == "System prompt"
    assert by_key["system"]["tokens"] == 200
    assert by_key["rules"]["tokens"] == 400
    assert by_key["memory"]["tokens"] == 50
    assert by_key["tools"]["tokens"] > 0
    assert by_key["conversation"]["tokens"] > 0
    # Reconciliation guarantees the breakdown sums to the headline used tokens.
    assert sum(entry["tokens"] for entry in categories) == payload["context_used"]


def test_categories_are_scaled_to_fit_when_rough_estimate_overshoots():
    """Rough char/4 estimates (esp. tool JSON) can exceed the real prompt size.

    Mirrors the observed UI bug where the legend summed to ~46.6K while the
    headline measured 42.7K. After the fix the buckets scale down to fit and
    no spurious Conversation bucket is appended.
    """

    class _SmallCompressor:
        last_prompt_tokens = 600
        context_length = 200_000
        compression_count = 0

    agent = SimpleNamespace(
        model="openai/gpt-5.5-pro",
        session_input_tokens=0,
        session_output_tokens=0,
        session_cache_read_tokens=0,
        session_cache_write_tokens=0,
        session_reasoning_tokens=0,
        session_prompt_tokens=0,
        session_completion_tokens=0,
        session_total_tokens=0,
        context_compressor=_SmallCompressor(),
        provider="openrouter",
        base_url="",
        tools=[{"type": "function", "function": {"name": "shell"}}],
    )

    fake_parts = {
        "stable": "a" * 800,  # ~200 tokens
        "context": "b" * 1600,  # ~400 tokens
        "volatile": "c" * 800,  # ~200 tokens
    }

    with patch(
        "agent.system_prompt.build_system_prompt_parts",
        return_value=fake_parts,
    ):
        payload = build_context_usage_payload(agent)

    categories = payload["categories"]
    by_key = {entry["key"]: entry for entry in categories}
    # Overhead (~800+ tokens) overshoots the measured 600, so it is scaled down
    # and there is no remainder to attribute to Conversation.
    assert "conversation" not in by_key
    assert sum(entry["tokens"] for entry in categories) == 600
    assert all(entry["tokens"] > 0 for entry in categories)


def test_scaling_sums_exactly_for_tiny_context():
    """Degenerate guard: even when the measured fill is smaller than the bucket
    count, the largest-remainder split sums exactly and never goes negative."""

    class _TinyCompressor:
        last_prompt_tokens = 2
        context_length = 200_000
        compression_count = 0

    agent = SimpleNamespace(
        model="m",
        session_input_tokens=0,
        session_output_tokens=0,
        session_cache_read_tokens=0,
        session_cache_write_tokens=0,
        session_reasoning_tokens=0,
        session_prompt_tokens=0,
        session_completion_tokens=0,
        session_total_tokens=0,
        context_compressor=_TinyCompressor(),
        provider="openrouter",
        base_url="",
        tools=[{"type": "function", "function": {"name": "shell"}}],
    )

    fake_parts = {"stable": "a" * 800, "context": "b" * 1600, "volatile": "c" * 800}

    with patch(
        "agent.system_prompt.build_system_prompt_parts",
        return_value=fake_parts,
    ):
        payload = build_context_usage_payload(agent)

    categories = payload["categories"]
    assert sum(entry["tokens"] for entry in categories) == 2
    assert all(entry["tokens"] >= 0 for entry in categories)


def test_build_context_usage_payload_omits_categories_when_no_sources():
    agent = SimpleNamespace(
        model="test",
        session_input_tokens=0,
        session_output_tokens=0,
        session_cache_read_tokens=0,
        session_cache_write_tokens=0,
        session_reasoning_tokens=0,
        session_prompt_tokens=0,
        session_completion_tokens=0,
        session_total_tokens=0,
        context_compressor=_FakeCompressor(),
        provider="openrouter",
        base_url="",
    )

    with patch(
        "agent.system_prompt.build_system_prompt_parts",
        side_effect=Exception("no system prompt builder for this agent shape"),
    ):
        payload = build_context_usage_payload(agent)

    assert "categories" not in payload


def test_emit_context_usage_invokes_callback():
    agent = SimpleNamespace(
        model="test",
        session_input_tokens=0,
        session_output_tokens=0,
        session_cache_read_tokens=0,
        session_cache_write_tokens=0,
        session_reasoning_tokens=0,
        session_prompt_tokens=0,
        session_completion_tokens=0,
        session_total_tokens=0,
        context_compressor=_FakeCompressor(),
        provider="openrouter",
        base_url="",
    )
    seen = []

    agent.context_usage_callback = seen.append
    emit_context_usage(agent)

    assert len(seen) == 1
    assert seen[0]["context_percent"] == 56
