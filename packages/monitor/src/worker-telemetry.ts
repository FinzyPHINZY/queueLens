import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import type { Job, Worker } from 'bullmq';
import type { Redis } from 'ioredis';
import type { WorkerObservation } from '../../core/src/monitor.js';

export interface WorkerTelemetryOptions {
  id?: string;
  hostname?: string;
  concurrency?: number;
  version?: string;
  heartbeatIntervalMs?: number;
  heartbeatTtlMs?: number;
  onError?: (error: Error) => void;
}

/** Optional, payload-free telemetry written by the worker to QueueHarbor-owned keys. */
export function instrumentWorker(worker: Worker, options: WorkerTelemetryOptions) {
  const id = options.id ?? randomUUID();
  const intervalMs = options.heartbeatIntervalMs ?? 5_000;
  const ttlMs = options.heartbeatTtlMs ?? 15_000;
  const concurrency = worker.concurrency ?? options.concurrency ?? 1;
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(id)) throw new Error('Invalid worker ID');
  if (options.hostname && options.hostname.length > 128)
    throw new Error('Worker hostname label is too long');
  if (options.version && options.version.length > 128)
    throw new Error('Worker version label is too long');
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 10_000)
    throw new Error('Worker concurrency must be between 1 and 10000');
  if (!Number.isInteger(intervalMs) || intervalMs < 1_000 || intervalMs > 60_000)
    throw new Error('Heartbeat interval must be between 1000 and 60000 ms');
  if (!Number.isInteger(ttlMs) || ttlMs < intervalMs * 2 || ttlMs > 300_000)
    throw new Error('Heartbeat TTL must be at least twice the interval and at most 300000 ms');

  const queueKey = encodeURIComponent(worker.name);
  const registryKey = `queuelens:workers:${queueKey}`;
  const observationKey = `${registryKey}:${id}`;
  const active = new Set<string>();
  let completed = 0;
  let failed = 0;
  let stalledSignals = 0;
  const events = new Map<number, { completed: number; failed: number }>();
  let closed = false;
  let currentWrite: Promise<void> | undefined;

  const onActive = (job: Job) => {
    if (job.id && active.size < 100) active.add(job.id);
  };
  const onCompleted = (job: Job) => {
    if (job.id) active.delete(job.id);
    completed++;
    const second = Math.floor(Date.now() / 1_000);
    const bucket = events.get(second) ?? { completed: 0, failed: 0 };
    bucket.completed++;
    events.set(second, bucket);
  };
  const onFailed = (job: Job | undefined) => {
    if (job?.id) active.delete(job.id);
    failed++;
    const second = Math.floor(Date.now() / 1_000);
    const bucket = events.get(second) ?? { completed: 0, failed: 0 };
    bucket.failed++;
    events.set(second, bucket);
  };
  const onStalled = (jobId: string) => {
    active.delete(jobId);
    stalledSignals++;
  };
  worker.on('active', onActive);
  worker.on('completed', onCompleted);
  worker.on('failed', onFailed);
  worker.on('stalled', onStalled);

  const heartbeat = (): Promise<void> => {
    if (closed) return Promise.resolve();
    if (currentWrite) return currentWrite;
    const operation = (async () => {
      try {
        const now = Date.now();
        const cutoff = Math.floor(now / 1_000) - 60;
        for (const second of events.keys()) if (second < cutoff) events.delete(second);
        const throughput = [...events.values()].reduce(
          (sum, bucket) => ({
            completed: sum.completed + bucket.completed,
            failed: sum.failed + bucket.failed,
          }),
          { completed: 0, failed: 0 },
        );
        const observation: WorkerObservation = {
          id,
          queue: worker.name,
          hostname: options.hostname ?? hostname(),
          concurrency,
          version: options.version,
          lastHeartbeat: now,
          heartbeatTtlMs: ttlMs,
          state: 'online',
          activeJobIds: [...active],
          completed,
          failed,
          throughput: { ...throughput, windowMs: 60_000, source: 'event-derived' },
          stalledSignals,
          source: 'instrumented',
        };
        const redis = (await worker.client) as unknown as Redis;
        await redis
          .multi()
          .set(observationKey, JSON.stringify(observation), 'PX', ttlMs * 3)
          .zadd(registryKey, now, id)
          .zremrangebyscore(registryKey, '-inf', now - 900_000)
          .exec();
      } catch (cause) {
        try {
          options.onError?.(
            cause instanceof Error ? cause : new Error('Telemetry heartbeat failed'),
          );
        } catch {
          // Telemetry must not fail the worker when an observer callback throws.
        }
      }
    })();
    currentWrite = operation;
    void operation.finally(() => {
      if (currentWrite === operation) currentWrite = undefined;
    });
    return operation;
  };
  const timer = setInterval(() => void heartbeat(), intervalMs);
  timer.unref();
  void heartbeat();

  return {
    id,
    heartbeat,
    close: async () => {
      closed = true;
      clearInterval(timer);
      worker.off('active', onActive);
      worker.off('completed', onCompleted);
      worker.off('failed', onFailed);
      worker.off('stalled', onStalled);
      await currentWrite;
      try {
        const redis = (await worker.client) as unknown as Redis;
        await redis.multi().del(observationKey).zrem(registryKey, id).exec();
      } catch {
        // A disconnected worker cannot remove its key; Redis TTL limits visibility.
      }
    },
  };
}
