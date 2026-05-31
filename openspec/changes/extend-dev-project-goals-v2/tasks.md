## 1. Slash commands

- [x] 1.1 Add `gateway/dev_control/project_goal_slash.py` dispatcher
- [x] 1.2 Wire `/project`, `/vision`, `/milestone`, `/pgoal`, `/psubgoal` in api_server
- [x] 1.3 Register commands in `hermes_cli/commands.py`
- [x] 1.4 Tests for create/tree/list

## 2. Config toggles

- [x] 2.1 Add `project_goals_config.py` (`tick_enabled`, `auto_subgoal_on_approve`)
- [x] 2.2 Document in `cli-config.yaml.example`

## 3. Plan artifact linking

- [x] 3.1 `maybe_create_subgoal_for_approved_artifact` on approve route
- [x] 3.2 Idempotent by `plan_artifact_id`

## 4. Evidence + dashboard

- [x] 4.1 Extend `assemble_evidence` with signals + reliability
- [x] 4.2 Include `project_goals` in project-dashboard payload + fingerprint

## 5. Docs

- [x] 5.1 Update `docs/dev-project-goals-spec.md` v2 section
