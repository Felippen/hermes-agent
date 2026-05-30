# Proposal: extend-dev-project-goals-v2

## Why

v1 shipped the store, API, CLI, judge loop, and lab tick gate. Operators still
needed gateway parity, dashboard visibility, config toggles, and tighter links
from approved plans into the goal hierarchy.

## What

- Slash commands for project goals on the Dev gateway
- `cli-config.yaml` toggles for tick and auto-subgoal
- Auto-create subgoals on plan artifact approve
- Richer evidence digest (production signals + reliability)
- Project dashboard `project_goals` read model field

## Impact

Oryn-owned modules under `gateway/dev_control/` plus small seams in
`gateway/platforms/api_server.py` and `hermes_cli/commands.py`.
