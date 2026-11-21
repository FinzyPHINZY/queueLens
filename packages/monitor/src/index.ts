import { Queue, type Queue as BullQueue } from 'bullmq';
import { fileURLToPath } from 'node:url';
import { BullMqQueueAdapter } from '../../bullmq-adapter/src/adapter.js';
import { redisOptions } from '../../bullmq-adapter/src/connection.js';
import { QueueRegistry } from '../../core/src/monitor.js';
import { SnapshotPoller } from '../../core/src/poller.js';
import { ReadBudget } from '../../core/src/read-budget.js';
import { validateDisclosurePolicy } from '../../core/src/redaction.js';
import { createHttpHandler, type HttpConfig } from '../../server/src/http.js';

export type { AuthTarget, Authorize, HttpConfig } from '../../server/src/http.js';
export type { ApiResponse, MonitorErrorCode, ResponseMeta } from '../../core/src/monitor.js';
export type { DisclosurePolicy } from '../../core/src/redaction.js';
export type { PollingOptions } from '../../core/src/poller.js';

export interface MonitorOptions extends Omit<HttpConfig, 'registry' | 'poller'> {
  queues: BullQueue[];
}

/** Embed a read-only monitor around queues owned by the host application. */
export function createMonitor(options: MonitorOptions) {
  if (options.queues.length === 0) throw new Error('At least one BullMQ queue is required');
  const registry = new QueueRegistry(options.queues.map((queue) => new BullMqQueueAdapter(queue)));
  const poller = new SnapshotPoller(registry, options.polling);
  const handler = createHttpHandler({
    ...options,
    dashboardDir: options.dashboardDir ?? defaultDashboardDir,
    registry,
    poller,
  });
  poller.start();
  return {
    handler,
    close: async () => {
      poller.stop();
      await registry.close();
    },
  };
}

/** Mount with Express: app.use('/monitor', monitor.handler). */
export const createExpressMonitor = createMonitor;

export interface StandaloneOptions extends Omit<HttpConfig, 'registry' | 'poller'> {
  redisUrl: string;
  queueNames: string[];
}

/** Create queues owned by this monitor; close() releases them on shutdown. */
export function createStandaloneMonitor(options: StandaloneOptions) {
  if (options.queueNames.length === 0) throw new Error('At least one queue name is required');
  if (
    options.queueNames.some((name) => !name) ||
    new Set(options.queueNames).size !== options.queueNames.length
  ) {
    throw new Error('Queue names must be nonempty and unique');
  }
  if (!options.token && !options.authorize)
    throw new Error('A monitor authorization strategy is required');
  validateDisclosurePolicy(options.redaction);
  new ReadBudget(options.polling?.maxConcurrentReads, options.polling?.queryTimeoutMs);
  const interval = options.polling?.intervalMs ?? 5_000;
  if (!Number.isInteger(interval) || interval < 1_000 || interval > 60_000) {
    throw new Error('MONITOR_POLL_INTERVAL_MS must be between 1000 and 60000');
  }
  const connection = redisOptions(options.redisUrl);
  const registry = new QueueRegistry(
    options.queueNames.map(
      (name) =>
        new BullMqQueueAdapter(
          new Queue(name, {
            connection: {
              ...connection,
              connectTimeout: options.polling?.queryTimeoutMs ?? 2_000,
              maxRetriesPerRequest: 1,
              enableOfflineQueue: false,
              retryStrategy: (attempts) => Math.min(30_000, 500 * 2 ** Math.min(attempts, 6)),
            },
          }),
          true,
        ),
    ),
  );
  const poller = new SnapshotPoller(registry, options.polling);
  const handler = createHttpHandler({
    ...options,
    dashboardDir: options.dashboardDir ?? defaultDashboardDir,
    registry,
    poller,
  });
  poller.start();
  return {
    handler,
    close: async () => {
      poller.stop();
      await registry.close();
    },
  };
}

const defaultDashboardDir = fileURLToPath(new URL('../../../dashboard/', import.meta.url));
