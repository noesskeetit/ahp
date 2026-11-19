export type JSONValue = null | boolean | number | string | JSONObject | JSONValue[];
export interface JSONObject { [key: string]: JSONValue }
export type QueryValue = string | number | boolean;
export interface JournalClientConfig {
  baseURL: string;
  caFile: string;
  certFile: string;
  keyFile: string;
  tokenSource: () => string | Promise<string>;
}
export interface JournalRequest {
  body?: object;
  query?: URLSearchParams | Record<string, QueryValue | readonly QueryValue[] | undefined>;
  signal?: AbortSignal;
}
export class JournalHTTPError extends Error {
  readonly status: number;
  readonly code: string;
  readonly outcomeUnknown: boolean;
  constructor(status: number, code: string, outcomeUnknown?: boolean);
}
export type SessionStatus = 'ACTIVE' | 'IDLE' | 'QUEUED' | 'WAITING_INPUT' | 'INTERRUPTED' | 'DELETED';
export type ReleaseStatus = 'IDLE' | 'WAITING_INPUT';
export interface BlobRef {
  artifact_id: string;
  version: string;
  sha256: string;
  size_bytes: number;
}
export type JournalPayload = {inline: string; blob?: never} | {inline?: null; blob: BlobRef};
export interface Lease {
  worker: string;
  epoch: number;
  expires_at: string;
  tool_lease_proof?: string;
}
export interface AcquireSessionRequest {
  session_id: string;
  harness_type: string;
  format_version: number;
  ttl_ms?: number;
  read_only?: false;
}
export interface ResumeState {
  session_id: string;
  harness_type: string;
  format_version: number;
  status: SessionStatus;
  head_seq: number;
  head_checkpoint_id?: string;
  leaf_entry_id?: string;
  append_parent_id?: string;
  parent_session_id?: string;
  unresolved_intents: ToolIntent[];
}
export interface HeartbeatOptions {
  ttlMs?: number;
  intervalMs?: number;
  controller?: AbortController;
}
export interface Heartbeat {
  readonly fenced: boolean;
  readonly error: unknown;
  readonly signal: AbortSignal;
  readonly done: Promise<void>;
  stop(): Promise<void>;
}
export interface EnqueueTurnRequest {
  session_id: string;
  harness_type: string;
  format_version: number;
  key: string;
  input: JournalPayload;
}
export interface EnqueueReceipt { session_id: string; head_seq: number }
export interface ClaimTurnRequest { session_id?: string; ttl_ms?: number }
export interface TurnLease extends Lease, EnqueueReceipt { input: JournalPayload; input_seq: number }
export interface TurnStatus { status: SessionStatus; lease_epoch: number; head_seq: number; input_seq?: number }
export interface SessionQuery { harness?: string; cursor?: string; limit?: number }
export interface SessionPage { session_ids: string[]; next_cursor?: string }
export interface CatalogQuery extends SessionQuery {
  harness: string;
  agent_id?: string;
  app_id?: string;
  user_id?: string;
  status?: Exclude<SessionStatus, 'DELETED'>;
  search_query?: string;
  label?: string;
  spawned_by?: string;
  configured_agent_id?: readonly string[];
  updated_after?: string;
  offset?: number;
}
export interface CatalogUpdate {
  agent_id?: string;
  native_key?: string;
  title?: string;
  prompt?: string;
  summary_metadata?: JSONObject;
  parent_session_id?: string;
}
export interface CatalogSession<S extends object = JSONObject> {
  session_id: string;
  agent_id?: string;
  native_key?: string;
  title?: string;
  status: Exclude<SessionStatus, 'DELETED'>;
  updated_at: string;
  created_at: string;
  head_seq: number;
  leaf_entry_id?: string;
  prompt_ref?: string;
  summary_metadata?: S;
  parent_session_id?: string;
}
export interface CatalogPage<S extends object = JSONObject> { sessions: CatalogSession<S>[]; next_cursor?: string }

export interface EventInput {
  id: string;
  parent_id?: string;
  type: string;
  role: string;
  codec: string;
  payload: JournalPayload;
  content_text?: string;
}
export interface JournalEvent extends EventInput {
  seq: number;
  is_active: boolean;
  is_compacted: boolean;
  otel_trace_id?: string;
  otel_span_id?: string;
}
export interface EventPatch { seq: number; is_active?: boolean; is_compacted?: boolean }
export interface TreePointers { leaf_entry_id?: string; append_parent_id?: string }
export interface PreparedIntent {
  call_id: string;
  tool_name: string;
  args_digest: string | readonly number[];
  idempotency_key: string;
}
export interface IntentTransition {
  call_id: string;
  status: 'DISPATCHED' | 'COMPLETED' | 'OUTCOME_UNKNOWN';
  gateway_operation_id: string;
  result?: JournalPayload;
}
export interface ToolIntent extends Omit<IntentTransition, 'status'> {
  status: IntentTransition['status'] | 'PREPARED';
  tool_name: string;
  args_digest: string;
}
export interface StandaloneToolRequest {
  lease_epoch: number;
  call_id: string;
  tool_name: string;
  args_digest: string | readonly number[];
  ack_lost?: boolean;
}
export interface ToolDecision extends IntentTransition { can_execute: boolean }
export interface AppendEventsRequest {
  lease_epoch: number;
  key: string;
  expected_seq: number;
  events: readonly EventInput[];
  kv?: readonly KVMutation[];
  intents?: readonly PreparedIntent[];
  tool_outcomes?: readonly IntentTransition[];
  pointers?: TreePointers;
  finish_turn?: ReleaseStatus;
  catalog?: CatalogUpdate;
  event_patches?: readonly EventPatch[];
}
export interface AppendReceipt { first_seq: number; head_seq: number }
export interface EventsQuery {
  after_seq?: number;
  limit?: number;
  q?: string;
  branch_leaf_id?: string;
  after_ts?: string;
  active_only?: boolean;
  num_recent?: number;
  state_namespace?: string;
}
export interface EventPage extends TreePointers {
  lease_epoch?: number;
  head_seq: number;
  events: JournalEvent[];
  state?: KVEntry[];
}
export interface PendingWrite {
  task_id: string;
  task_path?: string;
  index: number;
  channel: string;
  codec: string;
  payload: JournalPayload;
}
export interface Checkpoint {
  namespace: string;
  id: string;
  parent_id?: string;
  snapshot_seq: number;
  metadata: JSONObject;
  channel_versions: JSONObject;
  codec: string;
  payload: JournalPayload;
  chat_event_id?: string;
}
export interface CheckpointTuple { checkpoint: Checkpoint; writes: PendingWrite[] }
export interface CheckpointQuery { checkpoint_ns?: string; checkpoint_id?: string }
export interface CheckpointListQuery {
  checkpoint_ns?: string;
  before?: string;
  limit?: number;
  metadata?: JSONObject;
  include_tuples?: boolean;
  include_unindexed?: boolean;
}
export interface CheckpointPage { checkpoint_ids: string[]; checkpoint_tuples?: CheckpointTuple[] }
export interface PutCheckpointRequest {
  lease_epoch: number;
  key: string;
  checkpoint: Checkpoint;
  writes?: readonly PendingWrite[];
  intents?: readonly PreparedIntent[];
  tool_outcomes?: readonly IntentTransition[];
  chat?: ChatDelta;
}
export interface PutWritesRequest { lease_epoch: number; namespace: string; writes: readonly PendingWrite[] }
export interface ChatMessage { id: string; role: string; content: JSONValue }
export interface ChatDelta { upserts?: readonly ChatMessage[]; remove?: readonly string[]; order?: readonly string[] }
export interface ChatHistory { checkpoint_id: string; messages: ChatMessage[] }
export type KVScope = 'session' | 'user' | 'app' | 'store';
export interface KVQuery {
  scope_type: KVScope;
  namespace: string;
  session_id?: string;
  key?: string | readonly string[];
  prefix?: boolean;
  limit?: number;
  offset?: number;
}
export interface KVEntry {
  namespace?: string;
  scope_type: KVScope;
  key: string;
  version: number;
  payload: JournalPayload | {inline: null};
  deleted?: boolean;
  updated_at?: string;
}
export type KVMutation = {
  scope_type: KVScope;
  namespace: string;
  key: string;
  expected_version: number;
} & ({payload: JournalPayload; delete?: false} | {delete: true; payload?: JournalPayload});
export interface MutateKVRequest {
  session_id: string;
  lease_epoch: number;
  key: string;
  expected_seq?: number;
  kv?: readonly KVMutation[];
  catalog?: CatalogUpdate;
}

export class CompactJournalClient {
  constructor(config: JournalClientConfig | string);
  request<T = unknown>(method: 'GET' | 'POST' | 'PATCH' | 'DELETE', path: string, request?: JournalRequest): Promise<T>;
  requestTool<T = unknown>(operation: 'Call' | 'Get' | 'GetResult' | 'FenceAndLookup', body: object): Promise<T>;
  acquireSession(body: {session_id: string; read_only: true}): Promise<ResumeState>;
  acquireSession(body: AcquireSessionRequest): Promise<Lease & ResumeState>;
  heartbeatLease(id: string, epoch: number, options?: {ttlMs?: number; signal?: AbortSignal}): Promise<Lease>;
  releaseLease(id: string, epoch: number, status?: ReleaseStatus): Promise<void>;
  startHeartbeat(id: string, epoch: number, options?: HeartbeatOptions): Heartbeat;
  enqueueTurn(body: EnqueueTurnRequest): Promise<EnqueueReceipt>;
  claimTurn(body?: ClaimTurnRequest): Promise<TurnLease>;
  getTurnStatus(id: string): Promise<TurnStatus>;
  inspectSession(id: string): Promise<ResumeState>;
  listSessions(query?: SessionQuery): Promise<SessionPage>;
  listCatalog<S extends object = JSONObject>(query: CatalogQuery): Promise<CatalogPage<S>>;
  appendEvents(id: string, body: AppendEventsRequest): Promise<AppendReceipt>;
  getEvents(id: string, query?: EventsQuery): Promise<EventPage>;
  putCheckpoint(id: string, body: PutCheckpointRequest): Promise<{checkpoint_id: string}>;
  getCheckpoint(id: string, query?: CheckpointQuery): Promise<CheckpointTuple>;
  listCheckpoints(id: string, query: CheckpointListQuery & {include_tuples: true}):
    Promise<CheckpointPage & {checkpoint_tuples: CheckpointTuple[]}>;
  listCheckpoints(id: string, query?: CheckpointListQuery): Promise<CheckpointPage>;
  putWrites(id: string, checkpointId: string, body: PutWritesRequest): Promise<void>;
  getKV(query: KVQuery): Promise<KVEntry[]>;
  mutateKV(body: MutateKVRequest): Promise<AppendReceipt>;
  getChatHistory(id: string, query?: CheckpointQuery): Promise<ChatHistory>;
  stageBlob(id: string, epoch: number, data: Uint8Array): Promise<BlobRef>;
  readBlob(id: string, ref: BlobRef): Promise<Uint8Array>;
  beginStandaloneTool(id: string, body: StandaloneToolRequest): Promise<ToolDecision>;
  getToolIntent(sessionId: string, callId: string): Promise<ToolIntent>;
  getUserState(namespace: string): Promise<KVEntry[]>;
  mutateToolIntent(body: {session_id: string; lease_epoch: number; transition: IntentTransition}): Promise<void>;
  deleteSession(id: string): Promise<void>;
  setLegalHold(id: string, held: boolean): Promise<void>;
  close(): void;
}
export function storeJournalJSON(client: CompactJournalClient, path: string, epoch: number, value: unknown): Promise<JournalPayload>;
export function loadJournalJSON<T = unknown>(client: CompactJournalClient, path: string, value: JournalPayload): Promise<T>;
export function beginStandaloneTool(client: CompactJournalClient, path: string, epoch: number, callId: string,
                                    tool: string, argsDigest: Uint8Array): Promise<ToolDecision>;
