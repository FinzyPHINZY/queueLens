import type { Job, Queue } from 'bullmq';
import type { Redis } from 'ioredis';
import {
  observedStates,
  type JobMetadata,
  type JobPageQuery,
  type QueueCounts,
  type RawJob,
  type ReadOnlyQueue,
  type RawFlow,
  type WorkerObservation,
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

  async getOldestWaitingAt(): Promise<number | null> {
    const [job] = await this.queue.getJobs(['waiting'], 0, 0, true);
    return job?.timestamp ?? null;
  }

  async listWorkers(): Promise<WorkerObservation[]> {
    const redis = (await this.queue.client) as unknown as Redis;
    const registryKey = `queuelens:workers:${encodeURIComponent(this.name)}`;
    const ids = await redis.zrevrangebyscore(
      registryKey,
      '+inf',
      Date.now() - 900_000,
      'LIMIT',
      0,
      100,
    );
    if (ids.length === 0) return [];
    const values = await redis.mget(ids.map((id) => `${registryKey}:${id}`));
    const now = Date.now();
    return values.flatMap((value) => {
      if (!value) return [];
      try {
        const item = JSON.parse(value) as WorkerObservation;
        if (
          !/^[A-Za-z0-9._-]{1,128}$/.test(item.id) ||
          item.queue !== this.name ||
          typeof item.hostname !== 'string' ||
          item.hostname.length > 128 ||
          !Number.isInteger(item.concurrency) ||
          item.concurrency < 1 ||
          item.concurrency > 10_000 ||
          (item.version !== undefined &&
            (typeof item.version !== 'string' || item.version.length > 128)) ||
          !Number.isFinite(item.lastHeartbeat) ||
          item.lastHeartbeat > now + 60_000 ||
          !Number.isFinite(item.heartbeatTtlMs) ||
          item.heartbeatTtlMs < 2_000 ||
          item.heartbeatTtlMs > 300_000 ||
          !Array.isArray(item.activeJobIds) ||
          item.activeJobIds.length > 100 ||
          item.activeJobIds.some((id) => typeof id !== 'string' || id.length > 128) ||
          !Number.isSafeInteger(item.completed) ||
          item.completed < 0 ||
          !Number.isSafeInteger(item.failed) ||
          item.failed < 0 ||
          !Number.isSafeInteger(item.stalledSignals) ||
          item.stalledSignals < 0 ||
          item.throughput?.source !== 'event-derived' ||
          item.throughput.windowMs !== 60_000 ||
          !Number.isSafeInteger(item.throughput.completed) ||
          item.throughput.completed < 0 ||
          !Number.isSafeInteger(item.throughput.failed) ||
          item.throughput.failed < 0
        )
          return [];
        return [
          {
            id: item.id,
            queue: item.queue,
            hostname: item.hostname,
            concurrency: item.concurrency,
            version: item.version,
            lastHeartbeat: item.lastHeartbeat,
            heartbeatTtlMs: item.heartbeatTtlMs,
            state: now - item.lastHeartbeat <= item.heartbeatTtlMs ? 'online' : 'stale',
            activeJobIds: item.activeJobIds,
            completed: item.completed,
            failed: item.failed,
            stalledSignals: item.stalledSignals,
            throughput: item.throughput,
            source: 'instrumented',
          },
        ];
      } catch {
        return [];
      }
    });
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

  async getFlow(id: string): Promise<RawFlow | null> {
    const job = await this.queue.getJob(id);
    if (!job) return null;
    const dependencies = await job.getDependencies({
      processed: { count: 50 },
      unprocessed: { count: 50 },
      failed: { count: 50 },
    });
    const keys = [
      ...Object.keys(dependencies.processed ?? {}),
      ...(dependencies.unprocessed ?? []),
      ...(dependencies.failed ?? []),
    ];
    return {
      parent: job.parent?.id ? { id: job.parent.id, queueKey: job.parent.queueKey } : undefined,
      childKeys: keys.slice(0, 50),
      partial:
        keys.length > 50 ||
        Boolean(
          dependencies.nextProcessedCursor ||
            dependencies.nextUnprocessedCursor ||
            dependencies.nextFailedCursor,
        ),
    };
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
      stalledSignals: job.stalledSignals,
      parentJobId: job.parentJobId,
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
      stalledSignals: job.stalledCounter,
      parentJobId: job.parent?.id,
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
