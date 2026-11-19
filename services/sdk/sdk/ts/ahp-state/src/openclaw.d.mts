import type {CatalogUpdate, CompactJournalClient, JournalClientConfig} from './index.mjs';
import type {ToolGateway} from './deepseek.mjs';

export interface OpenClawMetadata { id: string; createdAt: string }
export interface OpenClawEntry { id: string; type: string; parentId: string | null; timestamp: string }
export interface OpenClawNativeStorage<M extends OpenClawMetadata, E extends OpenClawEntry> {
  getMetadata(): Promise<M>;
  getLeafId(): Promise<string | null>;
  getAppendParentId(): Promise<string | null>;
  createEntryId(): Promise<string>;
  getEntry(id: string): Promise<E | undefined>;
  getLabel(id: string): Promise<string | undefined>;
  getPathToRoot(id: string | null): Promise<E[]>;
  getEntries(): Promise<E[]>;
  appendEntry(entry: E): Promise<void>;
  setLeafId(id: string | null): Promise<void>;
}
export interface OpenClawNativeStorageCore<M extends OpenClawMetadata, E extends OpenClawEntry> {
  InMemorySessionStorage: new (options: {metadata: M; entries?: E[]}) => OpenClawNativeStorage<M, E>;
}
export interface OpenClawStorageOptions {
  ttlMs?: number;
  heartbeat?: boolean;
  effectTools?: Iterable<string>;
  catalog?: CatalogUpdate;
}
export class OpenClawOutcomeUnknown extends Error {
  readonly callID: string;
  constructor(callID: string);
}
export class CompactOpenClawStorage<
  M extends OpenClawMetadata = OpenClawMetadata,
  E extends OpenClawEntry = OpenClawEntry,
> implements OpenClawNativeStorage<M, E> {
  constructor(core: OpenClawNativeStorageCore<M, E>, client: Pick<CompactJournalClient, 'request'>,
    sessionId: string, options?: OpenClawStorageOptions);
  open(options?: {readOnly?: boolean}): Promise<this>;
  getMetadata(): Promise<M>;
  getLeafId(): Promise<string | null>;
  getAppendParentId(): Promise<string | null>;
  createEntryId(): Promise<string>;
  getEntry(id: string): Promise<E | undefined>;
  findEntries<T extends E['type']>(type: T): Promise<(string extends E['type'] ? E : Extract<E, {type: T}>)[]>;
  getLabel(id: string): Promise<string | undefined>;
  getPathToRoot(id: string | null): Promise<E[]>;
  getEntries(): Promise<E[]>;
  appendEntry(entry: E): Promise<void>;
  setLeafId(id: string | null): Promise<void>;
  dispatchTool(callID: string, name: string, args: unknown): Promise<string>;
  assertSafeResume(): Promise<void>;
  close(): Promise<void>;
}
export interface OpenClawAgentLifecycle {
  prompt(input: string): Promise<unknown>;
  continue(): Promise<unknown>;
}
export interface AttachedOpenClawAgent<A, S> {
  agent: A;
  session: S;
  unsubscribe(): void;
  prompt(input: string): Promise<void>;
  continue(): Promise<void>;
}
export function attachCompactOpenClawAgent<
  M extends OpenClawMetadata,
  E extends OpenClawEntry,
  O extends object,
  A extends OpenClawAgentLifecycle,
  S,
>(
  core: {
    Agent: new (options: O) => A;
    Session: new (storage: CompactOpenClawStorage<M, E>) => S;
  },
  storage: CompactOpenClawStorage<M, E>,
  options: O & {gateway?: ToolGateway},
): Promise<AttachedOpenClawAgent<A, S>>;

export interface OpenClawSessionListParams {
  limit?: number;
  offset?: number;
  cursor?: string;
  appId?: string;
  agentId?: string;
  userId?: string;
  status?: string;
  search?: string;
  label?: string;
  spawnedBy?: string;
  updatedAfter?: string;
  activeMinutes?: number;
  configuredAgentsOnly?: boolean;
  includeDerivedTitles?: boolean;
  includeLastMessage?: boolean;
}
export interface OpenClawSessionRow {
  key: string;
  kind: 'direct';
  sessionId: string;
  updatedAt: number;
  createdAt: number;
  displayName: string;
  label?: string;
  title?: string;
  lastMessagePreview?: string;
  status: string;
  headSeq: number;
  leafEntryId?: string;
  promptRef?: string;
  hasActiveRun: boolean;
}
export interface OpenClawSessionList {
  ts: number;
  path: 'ahp:state';
  count: number;
  limitApplied: number;
  defaults: Record<string, never>;
  hasMore: boolean;
  nextCursor?: string;
  nextOffset: number | null;
  sessions: OpenClawSessionRow[];
}
export interface OpenClawSessionHealth {
  path: 'ahp:state';
  count?: number;
  unavailable?: true;
  recent: {key: string; updatedAt: number; age: number}[];
}
export interface OpenClawGatewayCatalog {
  listJournalSessions(params: OpenClawSessionListParams, configuredAgents: readonly string[], defaultAgent: string,
    hasActiveRun?: (key: string) => boolean): Promise<OpenClawSessionList>;
  journalSessionHealth(agentId?: string): Promise<OpenClawSessionHealth>;
}
export function createOpenClawGatewayCatalog(client: Pick<CompactJournalClient, 'request'>): OpenClawGatewayCatalog;
export function registerOpenClawGatewayCatalog(options: {installDir: string; handshake: string | JournalClientConfig}):
  {deregister(): void};
