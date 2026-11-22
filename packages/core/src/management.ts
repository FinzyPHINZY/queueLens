import type { MonitorErrorCode } from './monitor.js';

export const managementActions = [
  'retry',
  'pause',
  'resume',
  'promote',
  'remove',
  'bulk-retry',
  'clean',
  'cancel-request',
] as const;
export type ManagementAction = (typeof managementActions)[number];
export type ManagementErrorCode =
  | 'INVALID_ACTOR'
  | 'INVALID_CONTENT_TYPE'
  | 'INVALID_CLEAN_FILTER'
  | 'INVALID_JOB_ID'
  | 'INVALID_TARGETS'
  | 'IDEMPOTENCY_KEY_REQUIRED'
  | 'IDEMPOTENCY_CONFLICT'
  | 'OPERATION_PENDING'
  | 'OPERATION_REJECTED'
  | 'PREVIEW_EXPIRED'
  | 'PREVIEW_MISMATCH'
  | 'PREVIEW_REQUIRED'
  | 'NO_ELIGIBLE_JOBS'
  | 'NOT_ELIGIBLE'
  | 'STATE_CHANGED'
  | 'QUEUE_BUSY'
  | 'MANAGEMENT_OUTCOME_UNCERTAIN'
  | 'BULLMQ_OPERATION_FAILED'
  | 'OUTCOME_UNCERTAIN';

export interface ManagementTarget {
  queueName?: string;
  permission: ManagementAction | 'audit:read' | 'preview:bulk-retry' | 'preview:clean';
}

export interface ManagementActor {
  id: string;
}

export interface AuditEvent {
  operationId: string;
  requestId: string;
  actorId: string;
  action: ManagementAction;
  queue: string;
  targetId?: string;
  phase: 'intent' | 'result';
  outcome?: 'succeeded' | 'rejected' | 'partial' | 'uncertain';
  code?: string;
  at: string;
}

export interface ManagementResult {
  operationId: string;
  action: ManagementAction;
  queue: string;
  status: 'succeeded' | 'partial';
  items?: { id: string; status: 'succeeded' | 'rejected' | 'uncertain'; code?: string }[];
}

export interface StoredOperation {
  fingerprint: string;
  status: 'pending' | 'complete';
  result?: ManagementResult;
  errorCode?: MonitorErrorCode;
}

export interface ManagementPreviewRecord {
  actorId: string;
  queue: string;
  action: 'bulk-retry' | 'clean';
  jobIds: string[];
  state?: 'completed' | 'failed';
  olderThanMs?: number;
  expiresAt: number;
}

export interface ManagementStore {
  getOperation(key: string): Promise<StoredOperation | null>;
  savePreview(token: string, preview: ManagementPreviewRecord): Promise<void>;
  loadPreview(token: string): Promise<ManagementPreviewRecord | null>;
  reserve(key: string, fingerprint: string, intent: AuditEvent): Promise<'new' | StoredOperation>;
  finish(key: string, operation: StoredOperation, event: AuditEvent): Promise<void>;
  recent(limit: number): Promise<AuditEvent[]>;
  countAttempt(scope: string, limit: number): Promise<boolean>;
  acquireQueue(queue: string, owner: string, ttlMs: number): Promise<boolean>;
  renewQueue(queue: string, owner: string, ttlMs: number): Promise<boolean>;
  releaseQueue(queue: string, owner: string): Promise<void>;
  requestCancellation(queue: string, jobId: string, ttlMs: number): Promise<void>;
  cancellationRequested(queue: string, jobId: string): Promise<boolean>;
  close(): Promise<void>;
}

export function validActorId(id: string): boolean {
  return /^[A-Za-z0-9._:@-]{1,128}$/.test(id);
}

export function validOperationKey(key: string): boolean {
  return /^[A-Za-z0-9._-]{8,128}$/.test(key);
}
