from gateway.dev_control.clarifications import (
    _normalize_project_context,
    _project_context_vision_hint,
)


def test_normalize_project_context_preserves_discovery_brief():
    context = _normalize_project_context({
        "project_id": "AlphaProject",
        "discovery_brief_markdown": "## Problem\nShip faster planning.",
    })
    assert context is not None
    assert context["discovery_brief_markdown"] == "## Problem\nShip faster planning."


def test_normalize_project_context_truncates_long_discovery_brief():
    long_brief = "x" * 9000
    context = _normalize_project_context({"project_id": "AlphaProject", "discovery_brief_markdown": long_brief})
    assert context is not None
    assert len(context["discovery_brief_markdown"]) <= 8001
    assert context["discovery_brief_markdown"].endswith("…")


def test_project_context_vision_hint_prefers_vision():
    hint = _project_context_vision_hint({
        "vision": "Primary vision",
        "discovery_brief_markdown": "Discovery brief",
    })
    assert hint == "Primary vision"


def test_project_context_vision_hint_falls_back_to_discovery_brief():
    hint = _project_context_vision_hint({
        "discovery_brief_markdown": "Discovery brief",
    })
    assert hint == "Discovery brief"
