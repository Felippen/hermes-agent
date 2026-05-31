# Design: add-dev-project-onboarding-a

## Clarification kind

Sessions carry `clarification_kind`: `planning` (default) or `project_onboarding`.
Stored in SQLite column plus session payload for API responses.

## Questions

Linear batch of five deterministic questions at start. Repo and constraints slots
are skippable. Planning LLM question generation is not used for this kind.

## Complete

`complete_clarification` builds `onboarding_profile` in `clarified_brief`:

- `project_name`, `intent_class`, `vision`, `repositories`, `constraints`,
  `non_goals`, `repos_deferred`

Invalid repo paths add a session warning but do not block completion.

## Workspace

Dashboard **Set up project** opens composer plan mode and starts onboarding
clarification immediately. On complete, Workspace updates `projects.json`,
creates a Hermes vision goal, and does not create a plan artifact.
