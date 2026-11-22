import { Queue, type Queue as BullQueue } from 'bullmq';
import { fileURLToPath } from 'node:url';
import { BullMqQueueAdapter } from '../../bullmq-adapter/src/adapter.js';
import { redisOptions } from '../../bullmq-adapter/src/connection.js';
import { QueueRegistry } from '../../core/src/monitor.js';
import { SnapshotPoller } from '../../core/src/poller.js';
import { ReadBudget } from '../../core/src/read-budget.js';
import { validateDisclosurePolicy } from '../../core/src/redaction.js';
import { createHttpHandler, type HttpConfig } from '../../server/src/http.js';
import { AlertEngine, validateAlertRules, type AlertRule } from '../../core/src/alerts.js';
import { HistoryCollector } from './history-collector.js';
import {
  RedisHistoryStore,
  validateHistoryStoreOptions,
  type HistoryStoreOptions,
} from './history-store.js';
import { NotificationDispatcher, type NotificationSink } from './notifications.js';
export { instrumentWorker, type WorkerTelemetryOptions } from './worker-telemetry.js';

export type { AuthTarget, Authorize, HttpConfig } from '../../server/src/http.js';
export type {
  ApiResponse,
  MonitorErrorCode,
  ResponseMeta,
  WorkerObservation,
} from '../../core/src/monitor.js';
export type { DisclosurePolicy } from '../../core/src/redaction.js';
export type { PollingOptions } from '../../core/src/poller.js';
export type { AlertRule } from '../../core/src/alerts.js';
export { createWebhookSink, createEmailSink } from './notifications.js';
export type { NotificationSink, EmailMessage } from './notifications.js';

export interface MonitorOptions
  extends Omit<HttpConfig, 'registry' | 'poller' | 'history' | 'alertStore' | 'alertRules'> {
  queues: BullQueue[];
  history?: HistoryStoreOptions;
  alertRules?: AlertRule[];
  notificationSinks?: NotificationSink[];
}

function historyServices(
  registry: QueueRegistry,
  poller: SnapshotPoller,
  options: MonitorOptions | StandaloneOptions,
) {
  const rules = options.alertRules ?? [];
  validateAlertRules(rules, registry.names());
  if (rules.length && !options.history) throw new Error('Alert rules require history storage');
  const store = options.history ? new RedisHistoryStore(options.history) : undefined;
  const engine = store ? new AlertEngine(store, rules, store.collectionIntervalMs) : undefined;
  const collector = store
    ? new HistoryCollector(registry, poller, store, async (sample) => {
        if (!engine) return;
        const queue = registry.get(sample.queue);
        const workers = await poller
          .runQueueRead(sample.queue, () => queue?.listWorkers?.() ?? Promise.resolve([]))
          .catch(() => []);
        await engine.evaluate(
          sample,
          new Map(workers?.map((worker) => [worker.id, worker.lastHeartbeat]) ?? []),
        );
      })
    : undefined;
  const dispatcher = store
    ? new NotificationDispatcher(store, options.notificationSinks ?? [])
    : undefined;
  return { store, collector, dispatcher, rules };
}

/** Embed a read-only monitor around queues owned by the host application. */
export function createMonitor(options: MonitorOptions) {
  if (options.queues.length === 0) throw new Error('At least one BullMQ queue is required');
  if (!options.token && !options.authorize)
    throw new Error('A monitor authorization strategy is required');
  validateDisclosurePolicy(options.redaction);
  const registry = new QueueRegistry(options.queues.map((queue) => new BullMqQueueAdapter(queue)));
  const poller = new SnapshotPoller(registry, options.polling);
  const history = historyServices(registry, poller, options);
  const handler = createHttpHandler({
    ...options,
    dashboardDir: options.dashboardDir ?? defaultDashboardDir,
    registry,
    poller,
    history: history.store,
    alertStore: history.store,
    alertRules: history.rules,
  });
  poller.start();
  history.collector?.start();
  history.dispatcher?.start();
  return {
    handler,
    close: async () => {
      poller.stop();
      history.collector?.stop();
      history.dispatcher?.stop();
      await history.store?.close();
      await registry.close();
    },
  };
}

/** Mount with Express: app.use('/monitor', monitor.handler). */
export const createExpressMonitor = createMonitor;

export interface StandaloneOptions
  extends Omit<HttpConfig, 'registry' | 'poller' | 'history' | 'alertStore' | 'alertRules'> {
  redisUrl: string;
  queueNames: string[];
  history?: HistoryStoreOptions;
  alertRules?: AlertRule[];
  notificationSinks?: NotificationSink[];
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
  validateAlertRules(options.alertRules ?? [], options.queueNames);
  if (options.alertRules?.length && !options.history)
    throw new Error('Alert rules require history storage');
  if (options.history) validateHistoryStoreOptions(options.history);
  new ReadBudget(options.polling?.maxConcurrentReads, options.polling?.queryTimeoutMs);
  const perQueue = options.polling?.maxReadsPerQueue ?? 2;
  if (!Number.isInteger(perQueue) || perQueue < 1 || perQueue > 8) {
    throw new Error('MONITOR_MAX_READS_PER_QUEUE must be between 1 and 8');
  }
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
  const history = historyServices(registry, poller, options);
  const handler = createHttpHandler({
    ...options,
    dashboardDir: options.dashboardDir ?? defaultDashboardDir,
    registry,
    poller,
    history: history.store,
    alertStore: history.store,
    alertRules: history.rules,
  });
  poller.start();
  history.collector?.start();
  history.dispatcher?.start();
  return {
    handler,
    close: async () => {
      poller.stop();
      history.collector?.stop();
      history.dispatcher?.stop();
      await history.store?.close();
      await registry.close();
    },
  };
}

const defaultDashboardDir = fileURLToPath(new URL('../../../dashboard/', import.meta.url));
