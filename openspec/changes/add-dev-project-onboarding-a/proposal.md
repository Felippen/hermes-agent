# Proposal: add-dev-project-onboarding-a

## Why

New Oryn projects are created with empty vision and no repos. Felipe needs a
guided Dev Q&A flow that materializes project profile fields and a Hermes vision
goal before feature planning begins.

## What

- `clarification_kind=project_onboarding` on durable Dev clarifications
- Deterministic five-question bank (name, intent, vision, repo, constraints)
- Deterministic onboarding profile on complete (no planning LLM synthesis)
- Oryn Workspace composer flow + dashboard **Set up project** CTA

## Impact

`gateway/dev_control/clarifications.py`, `gateway/dev_control/routes.py`,
`apps/oryn-workspace` (models, client, dashboard, composer)
