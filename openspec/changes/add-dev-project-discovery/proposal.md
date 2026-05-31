# Proposal: add-dev-project-discovery

## Why

Phases A–C provide structured project intake (6 deterministic questions) but not
interactive discovery. Felipe needs facilitated problem/vision co-creation with
a review gate before materializing project profile and vision goals.

## What

- New `clarification_kind`: `project_discovery`
- Adaptive LLM facilitator (seed + follow-up questions, up to 12 turns)
- Discovery brief synthesis with `brief_ready` status
- Approve / revise endpoints before materialization
- Workspace dual entry: **Define Project** (discovery) + **Quick Setup** (onboarding)

## Out of scope (V4b)

- `vault_search`, `web_search`, ADR citations

## Impact

`gateway/dev_control/clarifications.py`, `routes.py`, `apps/oryn-workspace`
