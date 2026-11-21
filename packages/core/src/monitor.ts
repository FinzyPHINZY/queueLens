export const observedStates = [
  'waiting',
  'active',
  'delayed',
  'completed',
  'failed',
  'paused',
  'waiting-children',
] as const;

export type ObservedState = (typeof observedStates)[number];
export type QueueCounts = Record<ObservedState, number>;

export interface JobMetadata {
  id: string;
  name: string;
  state: string;
  attemptsMade: number;
  timestamp: number;
  processedOn?: number;
  finishedOn?: number;
  delay?: number;
  priority?: number;
  progress?: unknown;
  repeatJobKey?: string;
  stalledSignals?: number;
  parentJobId?: string;
  queueWaitMs?: number;
  processingMs?: number;
}

export interface RawJob extends JobMetadata {
  data: unknown;
  returnValue: unknown;
  failedReason?: string;
  stacktrace: string[];
}

export interface JobPageQuery {
  state: ObservedState;
  offset: number;
  limit: number;
}

export interface WorkerObservation {
  id: string;
  queue: string;
  hostname: string;
  concurrency: number;
  version?: string;
  lastHeartbeat: number;
  heartbeatTtlMs: number;
  state: 'online' | 'stale';
  activeJobIds: string[];
  completed: number;
  failed: number;
  throughput: { completed: number; failed: number; windowMs: number; source: 'event-derived' };
  stalledSignals: number;
  source: 'instrumented';
}

export interface RawFlow {
  parent?: { id: string; queueKey: string };
  childKeys: string[];
  partial: boolean;
}

export interface SchedulerMetadata {
  key: string;
  name: string;
  next?: number;
  every?: number;
  pattern?: string;
  iterationCount?: number;
  limit?: number;
  endDate?: number;
  timeZone?: string;
}

export interface ReadOnlyQueue {
  readonly name: string;
  getCounts(): Promise<QueueCounts>;
  isPaused(): Promise<boolean>;
  listJobs(query: JobPageQuery): Promise<RawJob[]>;
  getJob(id: string): Promise<RawJob | null>;
  getJobMetadata(id: string): Promise<JobMetadata | null>;
  listWorkers?(): Promise<WorkerObservation[]>;
  getOldestWaitingAt?(): Promise<number | null>;
  getFlow?(id: string): Promise<RawFlow | null>;
  listSchedulers?(): Promise<SchedulerMetadata[]>;
  close?(): Promise<void>;
}

export interface QueueSnapshot {
  name: string;
  counts: QueueCounts;
  paused?: boolean;
  oldestWaitingAt?: number | null;
}

export interface ResponseMeta {
  observedAt: string;
  source: 'bullmq';
  freshnessMs: number;
  partial: boolean;
  warnings: string[];
}

export type MonitorErrorCode =
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'QUEUE_NOT_FOUND'
  | 'JOB_NOT_FOUND'
  | 'REDIS_UNAVAILABLE'
  | 'INVALID_REQUEST'
  | 'INVALID_FILTER'
  | 'RATE_LIMITED'
  | 'INTERNAL_ERROR';

export type ApiResponse<T> =
  | { success: true; data: T; meta: ResponseMeta }
  | {
      success: false;
      data: null;
      meta: ResponseMeta;
      error: { code: MonitorErrorCode; message: string };
    };

export function responseMeta(observedAt = new Date()): ResponseMeta {
  return {
    observedAt: observedAt.toISOString(),
    source: 'bullmq',
    freshnessMs: 0,
    partial: false,
    warnings: [],
  };
}

export class QueueRegistry {
  private readonly queues = new Map<string, ReadOnlyQueue>();

  constructor(queues: ReadOnlyQueue[]) {
    for (const queue of queues) {
      if (!queue.name || this.queues.has(queue.name)) {
        throw new Error('Queue names must be nonempty and unique');
      }
      this.queues.set(queue.name, queue);
    }
  }

  get(name: string): ReadOnlyQueue | undefined {
    return this.queues.get(name);
  }

  names(): string[] {
    return [...this.queues.keys()];
  }

  async snapshot(name: string): Promise<QueueSnapshot | null> {
    const queue = this.get(name);
    if (!queue) return null;
    const [counts, paused, oldestWaitingAt] = await Promise.all([
      queue.getCounts(),
      queue.isPaused(),
      queue.getOldestWaitingAt?.(),
    ]);
    return { name, counts, paused, oldestWaitingAt };
  }

  async close(): Promise<void> {
    await Promise.all([...this.queues.values()].map((queue) => queue.close?.()));
  }
}
