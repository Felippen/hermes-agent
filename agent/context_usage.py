"""Shared helpers for exposing agent context fill and session token usage."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def build_context_usage_payload(agent: Any) -> dict[str, Any]:
    """Build a normalized context-usage payload for HTTP/TUI consumers."""

    def _get(key: str, fallback: str | None = None) -> int:
        value = getattr(agent, key, 0) or 0
        if not value and fallback:
            value = getattr(agent, fallback, 0) or 0
        return int(value)

    session = {
        "input_tokens": _get("session_input_tokens", "session_prompt_tokens"),
        "output_tokens": _get("session_output_tokens", "session_completion_tokens"),
        "cache_read_tokens": _get("session_cache_read_tokens"),
        "cache_write_tokens": _get("session_cache_write_tokens"),
        "reasoning_tokens": _get("session_reasoning_tokens"),
        "prompt_tokens": _get("session_prompt_tokens"),
        "completion_tokens": _get("session_completion_tokens"),
        "total_tokens": _get("session_total_tokens"),
    }

    payload: dict[str, Any] = {
        "model": getattr(agent, "model", "") or "",
        "compressions": 0,
        "session": session,
    }

    compressor = getattr(agent, "context_compressor", None)
    if compressor is not None:
        ctx_used = (
            getattr(compressor, "last_prompt_tokens", 0) or session["total_tokens"] or 0
        )
        ctx_max = getattr(compressor, "context_length", 0) or 0
        payload["context_used"] = int(ctx_used)
        payload["context_max"] = int(ctx_max)
        if ctx_max:
            payload["context_percent"] = max(
                0,
                min(100, round(ctx_used / ctx_max * 100)),
            )
        else:
            payload["context_percent"] = 0
        payload["compressions"] = int(getattr(compressor, "compression_count", 0) or 0)
    else:
        payload["context_used"] = session["total_tokens"]
        payload["context_max"] = 0
        payload["context_percent"] = 0

    # Reconcile the rough per-bucket estimates against the measured
    # ``context_used`` so the UI breakdown sums to the headline number
    # (and the segmented bar tracks ``context_percent``) instead of drifting.
    categories = _reconcile_categories(
        _build_categories(agent), int(payload["context_used"])
    )
    if categories:
        payload["categories"] = categories

    try:
        from agent.usage_pricing import CanonicalUsage, estimate_usage_cost

        cost = estimate_usage_cost(
            payload["model"],
            CanonicalUsage(
                input_tokens=session["input_tokens"],
                output_tokens=session["output_tokens"],
                cache_read_tokens=session["cache_read_tokens"],
                cache_write_tokens=session["cache_write_tokens"],
            ),
            provider=getattr(agent, "provider", None),
            base_url=getattr(agent, "base_url", None),
        )
        payload["cost_status"] = cost.status
        if cost.amount_usd is not None:
            payload["cost_usd"] = float(cost.amount_usd)
    except Exception:
        pass

    return payload


def emit_context_usage(agent: Any) -> None:
    """Invoke ``context_usage_callback`` when configured."""
    callback = getattr(agent, "context_usage_callback", None)
    if not callable(callback):
        return
    try:
        callback(build_context_usage_payload(agent))
    except Exception:
        logger.debug("context_usage_callback failed", exc_info=True)


def _build_categories(agent: Any) -> list[dict[str, Any]]:
    """Best-effort token estimate for the stable prompt *overhead* buckets.

    Buckets reflect Hermes' prompt assembly: stable identity + guidance,
    project rules/context files, volatile memory, and tool schemas. Numbers
    are coarse (~4 chars/token) — exact counting would require
    provider-specific tokenizers and isn't worth the complexity for a UI
    indicator.

    The live conversation history is deliberately *not* estimated here: it is
    derived as the remainder of the measured ``context_used`` in
    :func:`_reconcile_categories`. Estimating it from ``agent.conversation_history``
    is unreliable — that snapshot is stale or cleared at several emit points
    (e.g. right after compression) — and a rough char count never matches the
    real prompt size, leaving the breakdown unable to reconcile.
    """
    try:
        from agent.model_metadata import estimate_tokens_rough
    except Exception:
        return []

    categories: list[dict[str, Any]] = []

    def _add(key: str, label: str, tokens: int) -> None:
        if tokens > 0:
            categories.append({"key": key, "label": label, "tokens": int(tokens)})

    try:
        from agent.system_prompt import build_system_prompt_parts

        parts = build_system_prompt_parts(agent)
    except Exception:
        parts = None

    if isinstance(parts, dict):
        _add(
            "system", "System prompt", estimate_tokens_rough(parts.get("stable") or "")
        )
        _add("rules", "Rules", estimate_tokens_rough(parts.get("context") or ""))
        _add("memory", "Memory", estimate_tokens_rough(parts.get("volatile") or ""))

    tools = getattr(agent, "tools", None) or []
    if tools:
        try:
            tools_json = json.dumps(tools, default=str)
            _add("tools", "Tool definitions", estimate_tokens_rough(tools_json))
        except Exception:
            pass

    return categories


def _reconcile_categories(
    categories: list[dict[str, Any]], context_used: int
) -> list[dict[str, Any]]:
    """Normalize the rough overhead buckets so they sum to ``context_used``.

    The headline ``context_used`` / ``context_percent`` come from the real,
    provider-reported prompt size, while the buckets are coarse ~4 char/token
    estimates. Left unscaled they routinely disagree with the headline and each
    other, so the UI breakdown (and its segmented bar) never reconciles. We
    treat the overhead buckets as *proportions* of the measured total:

    * if the overhead fits, the slack becomes the live ``conversation`` bucket;
    * if the rough estimate overshoots the real prompt (JSON tool schemas
      inflate the char/4 heuristic), the overhead buckets are scaled down to
      fit exactly.

    The result always sums to ``context_used``, so the bar fill matches
    ``context_percent`` and the legend matches the headline token count.
    """
    if context_used <= 0 or not categories:
        return categories

    overhead = sum(int(c["tokens"]) for c in categories)
    if overhead <= 0:
        return categories

    if overhead >= context_used:
        # Scale the overhead buckets to fit exactly using the largest-remainder
        # (Hare) method: floor each share, then hand the leftover tokens to the
        # buckets with the biggest fractional parts. This sums to context_used
        # by construction with no negative buckets — no drift correction needed.
        shares = [int(c["tokens"]) * context_used / overhead for c in categories]
        tokens = [int(s) for s in shares]  # floor (shares are non-negative)
        leftover = context_used - sum(tokens)  # 0 <= leftover < len(categories)
        for i in sorted(
            range(len(categories)),
            key=lambda i: shares[i] - tokens[i],
            reverse=True,
        )[:leftover]:
            tokens[i] += 1
        result = [{**c, "tokens": tokens[i]} for i, c in enumerate(categories)]
    else:
        # Overhead fits; the slack is the live conversation. Values are already
        # exact integers, so the breakdown sums to context_used exactly.
        result = [dict(c) for c in categories]
        result.append({
            "key": "conversation",
            "label": "Conversation",
            "tokens": context_used - overhead,
        })

    return [c for c in result if int(c["tokens"]) > 0]
