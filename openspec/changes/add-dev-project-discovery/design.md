# Design: add-dev-project-discovery

## Kind and statuses

- `clarification_kind`: `project_discovery`
- Status lifecycle: `active` → `brief_ready` → `completed` (on approve)
- `generation_mode`: `adaptive` (LLM) or `fallback`

## Limits

- `DISCOVERY_MIN_ANSWERS = 4`
- `DISCOVERY_MAX_QUESTIONS = 12`
- `DISCOVERY_SEED_QUESTIONS = 2`

## Adaptive advance

After each answer while `active`, facilitator LLM returns:

- `{ "action": "continue", "question": { ... } }` — append one question
- `{ "action": "ready", "reason": "..." }` — set `discovery_ready` in payload

Hard cap at 12 questions forces `can_complete`.

## Complete

`complete_clarification` for discovery synthesizes `clarified_brief` (discovery brief
schema), sets `status = brief_ready`. Does not set `completed_at`.

## Approve / revise

- `POST .../approve-brief` — `status → completed`, `brief_approved_at`
- `POST .../revise-brief` — `{ "feedback": "..." }` regenerates brief, stays `brief_ready`

## Discovery brief schema (`clarified_brief`)

`discovery_brief_version`, `project_name`, `problem`, `problem_evidence[]`,
`vision`, `success_criteria[]`, `users_operators[]`, `scope_in[]`, `scope_out[]`,
`parking_lot[]`, `assumptions[]`, `risks[]`, `open_questions[]`, `first_bet`,
`repositories[]`, `constraints[]`, `non_goals[]`, `intent_class`,
`suggested_next_action`

## Workspace UX

- Primary CTA: **Define Project** → `project_discovery`
- Secondary: **Quick Setup** → `project_onboarding` (unchanged)
- Composer shows brief review when `brief_ready`
- Materialize only after approve

## Facilitator principles

Probe problem before solution; challenge vague vision; reference repo grounding
when bound; stop when problem + vision + success + first bet are good enough.
