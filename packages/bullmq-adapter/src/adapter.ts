import type { Queue } from 'bullmq';
import {
  observedStates,
  type JobMetadata,
  type QueueCounts,
  type ReadOnlyQueue,
} from '../../core/src/monitor.js';

export class BullMqQueueAdapter implements ReadOnlyQueue {
  readonly name: string;

  constructor(
    private readonly queue: Queue,
    private readonly ownsQueue = false,
  ) {
    this.name = queue.name;
  }

  async getCounts(): Promise<QueueCounts> {
    const result = await this.queue.getJobCounts(...observedStates);
    return Object.fromEntries(
      observedStates.map((state) => [state, result[state] ?? 0]),
    ) as QueueCounts;
  }

  async getJobMetadata(id: string): Promise<JobMetadata | null> {
    const job = await this.queue.getJob(id);
    if (!job) return null;
    const state = await this.queue.getJobState(id);
    if (state === 'unknown') return null;
    return {
      id: job.id ?? id,
      name: job.name,
      state,
      attemptsMade: job.attemptsMade,
      timestamp: job.timestamp,
      processedOn: job.processedOn,
      finishedOn: job.finishedOn,
    };
  }

  async close(): Promise<void> {
    if (this.ownsQueue) await this.queue.close();
  }
}
