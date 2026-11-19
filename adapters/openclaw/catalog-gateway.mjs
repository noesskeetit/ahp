import {CompactJournalClient} from '../journal/compact-client.mjs';

let client;

// The Gateway process is provisioned with one AHP scope. Gateway operator
// permissions are checked by the native router before this handler runs.
export async function listJournalSessions(params, configuredAgents, defaultAgent, hasActiveRun) {
  client ??= new CompactJournalClient(process.env.AHP_OPENCLAW_CATALOG_HANDSHAKE);
  const limit = Math.min(100, Math.max(1, params.limit ?? 100));
  const query = new URLSearchParams({view: 'catalog', harness: 'openclaw', limit});
  const filters = {
    agent_id: params.agentId, app_id: params.appId, user_id: params.userId, status: params.status,
    search_query: params.search, label: params.label, spawned_by: params.spawnedBy, cursor: params.cursor,
  };
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== '') query.set(key, value);
  }
  if (params.configuredAgentsOnly) {
    for (const agent of configuredAgents) query.append('configured_agent_id', agent);
  }
  if (params.offset) query.set('offset', params.offset);
  let updatedAfter = params.updatedAfter;
  if (params.activeMinutes !== undefined) {
    const cutoff = new Date(Date.now() - Math.max(1, params.activeMinutes) * 60000).toISOString();
    if (!updatedAfter || Date.parse(cutoff) > Date.parse(updatedAfter)) updatedAfter = cutoff;
  }
  if (updatedAfter) query.set('updated_after', updatedAfter);
  const page = await client.request('GET', '/v1/state/sessions', {query});
  const sessions = page.sessions.map(row => {
    const key = row.native_key ?? `agent:${row.agent_id ?? defaultAgent}:${row.session_id}`;
    return {
      key, kind: 'direct', sessionId: row.session_id, updatedAt: Date.parse(row.updated_at),
      createdAt: Date.parse(row.created_at), displayName: row.title || key, label: row.summary_metadata?.label,
      title: params.includeDerivedTitles ? row.title : undefined,
      lastMessagePreview: params.includeLastMessage ? row.summary_metadata?.last_message_preview : undefined,
      status: row.status, headSeq: row.head_seq, leafEntryId: row.leaf_entry_id, promptRef: row.prompt_ref,
      hasActiveRun: row.status === 'ACTIVE' || hasActiveRun(key),
    };
  });
  return {
    ts: Date.now(), path: 'ahp:state', count: sessions.length, limitApplied: limit, defaults: {},
    hasMore: Boolean(page.next_cursor), nextCursor: page.next_cursor,
    nextOffset: page.next_cursor && !params.cursor ? (params.offset ?? 0) + sessions.length : null, sessions,
  };
}

export async function journalSessionHealth(agentId) {
  const now = Date.now();
  try {
    const page = await listJournalSessions({agentId, limit: 5}, [], agentId ?? 'main', () => false);
    return {
      path: page.path, count: page.hasMore ? undefined : page.count,
      recent: page.sessions.map(row => ({key: row.key, updatedAt: row.updatedAt, age: now - row.updatedAt})),
    };
  } catch {
    return {path: 'ahp:state', unavailable: true, recent: []};
  }
}
