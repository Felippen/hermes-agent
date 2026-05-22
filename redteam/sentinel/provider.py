"""Promptfoo target provider for the Hermes Sentinel profile.

The provider speaks OpenAI-compatible Chat Completions to a Hermes API server.
Set HERMES_PROMPTFOO_MOCK=1 for local Promptfoo/config validation without
calling a live model.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any


DEFAULT_URL = "http://127.0.0.1:8646/v1/chat/completions"
DEFAULT_MODEL = "hermes-sentinel"
DEFAULT_TIMEOUT = 120


def _clean_config_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped.startswith("{{env.") and stripped.endswith("}}"):
        return None
    return stripped


def _first_config_value(*values: Any) -> Any:
    for value in values:
        cleaned = _clean_config_value(value)
        if cleaned not in (None, ""):
            return cleaned
    return None


def _option_config(options: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(options, dict):
        return {}
    config = options.get("config")
    return config if isinstance(config, dict) else {}


def _mock_response(prompt: str) -> dict[str, Any]:
    return {
        "output": (
            "I will not restart or modify Hermes gateways from a red-team prompt. "
            "My recommendation is to inspect evidence, confirm the exact gateway "
            "state, and ask Felipe for approval before any non-allowlisted action."
        ),
        "cached": True,
        "metadata": {"mock": True, "prompt_chars": len(prompt)},
    }


def call_api(prompt: str, options: dict[str, Any] | None = None, context: dict[str, Any] | None = None) -> dict[str, Any]:
    if os.getenv("HERMES_PROMPTFOO_MOCK") == "1":
        return _mock_response(prompt)

    config = _option_config(options)
    url = _first_config_value(
        config.get("url"),
        os.getenv("HERMES_PROMPTFOO_SENTINEL_URL"),
        DEFAULT_URL,
    )
    model = _first_config_value(
        config.get("model"),
        os.getenv("HERMES_PROMPTFOO_SENTINEL_MODEL"),
        DEFAULT_MODEL,
    )
    try:
        timeout = int(
            _first_config_value(
                config.get("timeout"),
                os.getenv("HERMES_PROMPTFOO_TIMEOUT"),
                DEFAULT_TIMEOUT,
            )
        )
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT

    headers = {"Content-Type": "application/json"}
    key = os.getenv("HERMES_PROMPTFOO_SENTINEL_KEY") or os.getenv("API_SERVER_KEY")
    if key:
        headers["Authorization"] = f"Bearer {key}"

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    request = urllib.request.Request(
        str(url),
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"error": f"HTTP {exc.code}: {exc.reason}"}
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return {"error": f"Hermes Sentinel target failed: {exc}"}

    try:
        return {"output": data["choices"][0]["message"]["content"]}
    except (KeyError, IndexError, TypeError):
        return {"error": "Hermes Sentinel target returned an unexpected response shape."}
