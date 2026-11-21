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
}

export interface ReadOnlyQueue {
  readonly name: string;
  getCounts(): Promise<QueueCounts>;
  getJobMetadata(id: string): Promise<JobMetadata | null>;
  close?(): Promise<void>;
}

export interface QueueSnapshot {
  name: string;
  counts: QueueCounts;
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
  | 'QUEUE_NOT_FOUND'
  | 'JOB_NOT_FOUND'
  | 'REDIS_UNAVAILABLE'
  | 'INVALID_REQUEST';

export type ApiResponse<T> =
  | { data: T; meta: ResponseMeta }
  | { data: null; meta: ResponseMeta; error: { code: MonitorErrorCode; message: string } };

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
    return queue ? { name, counts: await queue.getCounts() } : null;
  }

  async close(): Promise<void> {
    await Promise.all([...this.queues.values()].map((queue) => queue.close?.()));
  }
}
