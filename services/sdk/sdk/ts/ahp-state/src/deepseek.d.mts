import type {CompactJournalClient, JournalEvent, TurnControlDelta, TurnLease} from './index.mjs';

export interface DeepSeekSessionHeader {
  readonly version: 4;
  readonly id: string;
  readonly createdAt: number;
  readonly cwd?: string;
  readonly parentSession?: string;
  readonly isSeeded: boolean;
  readonly origin?: 'subagent';
  readonly delegationDepth?: number;
  readonly agentPreset?: string;
}
export interface DeepSeekSessionEvent {
  readonly seq: number;
  readonly time: number;
  readonly type: string;
  readonly data: unknown;
  readonly ignorable?: true;
  readonly surfaceOp?: 'append' | {readonly op: 'replace'; readonly startSeq: number; readonly endSeq: number};
  readonly sourceEventSeqs?: readonly number[];
}
export type ToolGateway = (operation: 'Call' | 'Get' | 'GetResult', body: object) => Promise<unknown>;
export interface DeepSeekUserMessage {
  readonly id: string;
  readonly role: 'user';
  readonly source: {readonly kind: string};
  readonly content: readonly unknown[];
}
export interface DeepSeekPreclaimedTurn extends Pick<TurnLease, 'session_id' | 'epoch' | 'input_seq' | 'head_seq'> {
  /** For VM-managed turns, set this to input_seq. */
  readonly turn_seq?: number;
}
export interface DeepSeekPersistenceOptions {
  client: Pick<CompactJournalClient, 'request'>;
  prefix?: string;
  ttlMs?: number;
  heartbeat?: boolean;
  effectTools?: Iterable<string>;
  gateway?: ToolGateway;
  /** Supplies the host's lease to native AgentLoop create/resume. */
  preclaimedTurn?: (id: string) => DeepSeekPreclaimedTurn | undefined;
  /** Decodes the producer's opaque steer payload as a native message with source.kind 'user'. */
  decodeSteer?: (event: JournalEvent) => DeepSeekUserMessage | Promise<DeepSeekUserMessage>;
}
export interface DeepSeekSessionSnapshot<H extends DeepSeekSessionHeader = DeepSeekSessionHeader> {
  readonly header: H;
  readonly formatStatus: 'current';
  readonly eventCount?: number;
  readonly revision: string;
}
export interface DeepSeekOperationOptions { readonly signal?: AbortSignal }
export interface DeepSeekOpenOptions extends DeepSeekOperationOptions {
  /** The host owns renewal and terminal disposition of this queued turn. */
  readonly preclaimedTurn?: DeepSeekPreclaimedTurn;
}
export interface DeepSeekCreateOptions extends DeepSeekOpenOptions { readonly inheritedEventCount?: number }
export type DeepSeekAccess = 'read' | 'write';
export interface DeepSeekSessionHandle<
  H extends DeepSeekSessionHeader = DeepSeekSessionHeader,
  E extends DeepSeekSessionEvent = DeepSeekSessionEvent,
> {
  readonly id: string;
  readonly header: H;
  readonly access: DeepSeekAccess;
  readonly inheritedEventCount: number;
  read(offset?: number, length?: number, options?: DeepSeekOperationOptions):
    Promise<{eventState: 'detached'; events: readonly E[]}>;
  append(events: readonly E[], options?: DeepSeekOperationOptions): Promise<void>;
  flush(options?: DeepSeekOperationOptions): Promise<void>;
  close(): Promise<void>;
}
export interface DeepSeekSessionPersistence<
  H extends DeepSeekSessionHeader = DeepSeekSessionHeader,
  E extends DeepSeekSessionEvent = DeepSeekSessionEvent,
> {
  readonly identity: symbol;
  create(header: H, options?: DeepSeekCreateOptions): Promise<DeepSeekSessionHandle<H, E>>;
  open(id: string, access: DeepSeekAccess, options?: DeepSeekOpenOptions): Promise<DeepSeekSessionHandle<H, E>>;
  stat(id: string, options?: DeepSeekOperationOptions): Promise<DeepSeekSessionSnapshot<H> | undefined>;
  list(options?: DeepSeekOperationOptions): Promise<DeepSeekSessionSnapshot<H>[]>;
  dispatchTool(id: string, callId: string): Promise<string>;
  /** Forward renewal control to the writer for the specific turn. */
  acceptTurnControl(id: string, turnSeq: number, control: TurnControlDelta): void;
  /** Wake an idle native driver for pending steer while the same AHP turn is active. */
  resumePendingSteer(id: string, turnSeq: number): Promise<boolean>;
  flush(): Promise<void>;
  dispose(): Promise<void>;
}
export interface DeepSeekPersistenceConstructor {
  new <H extends DeepSeekSessionHeader = DeepSeekSessionHeader, E extends DeepSeekSessionEvent = DeepSeekSessionEvent>(
    ctx: object, options: DeepSeekPersistenceOptions,
  ): DeepSeekSessionPersistence<H, E>;
}
export function loadDeepSeekSessionPersistence(): Promise<DeepSeekPersistenceConstructor>;
export function createDeepSeekSessionPersistence<
  H extends DeepSeekSessionHeader = DeepSeekSessionHeader,
  E extends DeepSeekSessionEvent = DeepSeekSessionEvent,
>(ctx: object, options: DeepSeekPersistenceOptions): Promise<DeepSeekSessionPersistence<H, E>>;
