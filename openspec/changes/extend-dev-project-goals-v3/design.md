# Design: extend-dev-project-goals-v3

## Execution sync

After `create_execution_plan_from_artifact`, call `sync_subgoal_plan_id` to
write `payload.plan_id` on the linked subgoal. Re-evaluation may set `blocked`
when execution/verification/CI evidence indicates failure (config-gated).

## Overlay

Client sends `project_goal_tree_digest` in chat body; server appends to
`build_chat_project_context_overlay`. Workspace builds digest from decoded tree.

## Cache

Dev mutation handlers call `_invalidate_ao_read_models()` so ETag fingerprints
refresh without stale in-process cache.

## UI

Workspace decodes `project_goals` from project-dashboard; new section card with
nested rows — separate from Clarify→Launch pipeline bar.
