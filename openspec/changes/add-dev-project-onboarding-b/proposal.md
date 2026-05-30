# Proposal: add-dev-project-onboarding-b

## Why

Phase A configures projects. Felipe still starts feature planning with a blank
composer and generic LLM clarify questions instead of a scoped feature intake.

## What

- `clarification_kind=feature_onboarding` with deterministic questions
- Repo-focus question only when the project binds multiple repositories
- Completion builds a planning-ready clarified brief plus feature metadata
- Workspace **Start Planning** auto-starts feature onboarding, materializes a
  work item + goal under the project vision, then creates a plan artifact

## Impact

`gateway/dev_control/clarifications.py`, `apps/oryn-workspace`
