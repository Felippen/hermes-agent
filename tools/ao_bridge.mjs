#!/usr/bin/env node
import { pathToFileURL } from "node:url";

const AO_ROOT =
  process.env.AO_NODE_ROOT ||
  "/opt/homebrew/lib/node_modules/@composio/agent-orchestrator/node_modules";

async function readStdinJSON() {
  let input = "";
  for await (const chunk of process.stdin) {
    input += chunk;
  }
  return input.trim() ? JSON.parse(input) : {};
}

async function loadAO() {
  const core = await import(
    pathToFileURL(`${AO_ROOT}/@composio/ao-core/dist/config.js`).href
  );
  const cli = await import(
    pathToFileURL(`${AO_ROOT}/@composio/ao-cli/dist/lib/create-session-manager.js`).href
  );
  return { loadConfig: core.loadConfig, getSessionManager: cli.getSessionManager };
}

function normalizeSession(session) {
  if (!session) return null;
  const runtimeHandle = session.runtimeHandle || null;
  const metadata = session.metadata || {};
  return {
    id: session.id,
    project_id: session.projectId,
    status: session.status,
    activity: session.activity,
    branch: session.branch,
    issue_id: session.issueId,
    workspace_path: session.workspacePath,
    tmux_name: metadata.tmuxName || runtimeHandle?.id || null,
    agent: metadata.agent || null,
    model: null,
    pr: session.pr?.url || session.pr || null,
    summary: session.agentInfo?.summary || metadata.summary || null,
    created_at: session.createdAt,
    last_activity_at: session.lastActivityAt,
    runtime_handle: runtimeHandle,
    open_command: metadata.tmuxName || runtimeHandle?.id
      ? `tmux attach -t ${metadata.tmuxName || runtimeHandle.id}`
      : null,
  };
}

async function main() {
  const command = process.argv[2];
  const input = await readStdinJSON();
  const { loadConfig, getSessionManager } = await loadAO();
  const config = loadConfig(input.config_path || process.env.AO_CONFIG_PATH);
  const sm = await getSessionManager(config);

  if (command === "spawn") {
    const session = await sm.spawn({
      projectId: input.project_id,
      issueId: input.issue_id || undefined,
      prompt: input.prompt || undefined,
      branch: input.branch || undefined,
      agent: input.agent || undefined,
    });
    console.log(JSON.stringify({ ok: true, session: normalizeSession(session) }));
    return;
  }

  if (command === "status") {
    const session = await sm.get(input.session_id);
    console.log(JSON.stringify({ ok: Boolean(session), session: normalizeSession(session) }));
    return;
  }

  if (command === "kill") {
    await sm.kill(input.session_id);
    console.log(JSON.stringify({ ok: true, session_id: input.session_id }));
    return;
  }

  if (command === "send") {
    await sm.send(input.session_id, input.message || "");
    const session = await sm.get(input.session_id);
    console.log(JSON.stringify({ ok: true, session: normalizeSession(session) }));
    return;
  }

  if (command === "list") {
    const sessions = await sm.list(input.project_id || undefined);
    console.log(JSON.stringify({ ok: true, sessions: sessions.map(normalizeSession) }));
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

main().catch((error) => {
  console.error(JSON.stringify({ ok: false, error: String(error?.message || error) }));
  process.exit(1);
});
