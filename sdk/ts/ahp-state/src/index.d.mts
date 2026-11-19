export interface JournalClientConfig {
  baseURL: string;
  caFile: string;
  certFile: string;
  keyFile: string;
  tokenSource: () => string | Promise<string>;
}
export interface JournalRequest {
  body?: object;
  query?: Record<string, string | number | boolean>;
}
export class JournalHTTPError extends Error {
  readonly status: number;
  readonly code: string;
  readonly outcomeUnknown: boolean;
  constructor(status: number, code: string, outcomeUnknown?: boolean);
}
export class CompactJournalClient {
  constructor(config: JournalClientConfig | string);
  request<T>(method: 'GET' | 'POST' | 'PATCH' | 'DELETE', path: string, request?: JournalRequest): Promise<T>;
  requestTool<T>(operation: 'Call' | 'Get' | 'GetResult' | 'FenceAndLookup', body: object): Promise<T>;
  close(): void;
}
export interface BlobRef {
  artifact_id: string;
  version: string;
  sha256: string;
  size_bytes: number;
}
export type JournalPayload = {inline: string; blob?: never} | {inline?: null; blob: BlobRef};
export interface ToolDecision {
  call_id: string;
  status: 'DISPATCHED' | 'COMPLETED' | 'OUTCOME_UNKNOWN';
  can_execute: boolean;
  gateway_operation_id: string;
  result?: JournalPayload;
}
export function storeJournalJSON(client: CompactJournalClient, path: string, epoch: number, value: unknown): Promise<JournalPayload>;
export function loadJournalJSON<T>(client: CompactJournalClient, path: string, value: JournalPayload): Promise<T>;
export function beginStandaloneTool(client: CompactJournalClient, path: string, epoch: number, callId: string,
                                    tool: string, argsDigest: Uint8Array): Promise<ToolDecision>;
