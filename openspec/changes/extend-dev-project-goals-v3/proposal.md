# Proposal: extend-dev-project-goals-v3

## Why

V1–V2 persist and expose project goals via API and dashboard JSON, but the product
does not render them, Dev chat lacks a goal digest, and execution plans do not
sync back to subgoals.

## What

- Execution loop: `plan_id` sync on build/launch; blocked status from failures
- Coordinator overlay + dashboard cache invalidation
- PATCH goals + CLI/slash update
- Oryn Workspace goal tree UI (Oryn repo)

## Impact

`gateway/dev_control/*`, `hermes_cli/dev_goals.py`, `apps/oryn-workspace` (Oryn)
