import type { Job, Queue } from 'bullmq';
import {
  observedStates,
  type JobMetadata,
  type JobPageQuery,
  type QueueCounts,
  type RawJob,
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

  async isPaused(): Promise<boolean> {
    return this.queue.isPaused();
  }

  async listJobs(query: JobPageQuery): Promise<RawJob[]> {
    const jobs = await this.queue.getJobs(
      [query.state],
      query.offset,
      query.offset + query.limit - 1,
      false,
    );
    return jobs.map((job) => this.mapJob(job, query.state));
  }

  async getJob(id: string): Promise<RawJob | null> {
    const job = await this.queue.getJob(id);
    if (!job) return null;
    const state = await this.queue.getJobState(id);
    if (state === 'unknown') return null;
    return this.mapJob(job, state);
  }

  async getJobMetadata(id: string): Promise<JobMetadata | null> {
    const job = await this.getJob(id);
    if (!job) return null;
    return {
      id: job.id,
      name: job.name,
      state: job.state,
      attemptsMade: job.attemptsMade,
      timestamp: job.timestamp,
      processedOn: job.processedOn,
      finishedOn: job.finishedOn,
      delay: job.delay,
      priority: job.priority,
      progress: job.progress,
      repeatJobKey: job.repeatJobKey,
    };
  }

  private mapJob(job: Job, state: string): RawJob {
    return {
      id: job.id ?? '',
      name: job.name,
      state,
      attemptsMade: job.attemptsMade,
      timestamp: job.timestamp,
      processedOn: job.processedOn,
      finishedOn: job.finishedOn,
      delay: job.delay,
      priority: job.opts?.priority,
      progress: job.progress,
      repeatJobKey: job.repeatJobKey,
      data: job.data,
      returnValue: job.returnvalue,
      failedReason: job.failedReason,
      stacktrace: job.stacktrace ?? [],
    };
  }

  async close(): Promise<void> {
    if (this.ownsQueue) await this.queue.close();
  }
}
