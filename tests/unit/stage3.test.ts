import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
  AlertEngine,
  type AlertIncident,
  type AlertRule,
  type AlertStorage,
  type NotificationEvent,
  type RuleState,
} from '../../packages/core/src/alerts.js';
import type { HistorySample } from '../../packages/core/src/history.js';
import {
  NotificationDispatcher,
  createEmailSink,
  createWebhookSink,
} from '../../packages/monitor/src/notifications.js';
import { QueueRegistry, type ReadOnlyQueue } from '../../packages/core/src/monitor.js';
import { createHttpHandler } from '../../packages/server/src/http.js';
import { HistoryCollector } from '../../packages/monitor/src/history-collector.js';
import { SnapshotPoller } from '../../packages/core/src/poller.js';
import type { RedisHistoryStore } from '../../packages/monitor/src/history-store.js';
import { createStandaloneMonitor } from '../../packages/monitor/src/index.js';

class MemoryStore implements AlertStorage {
  states = new Map<string, RuleState>();
  incidents = new Map<string, AlertIncident>();
  events = new Map<string, NotificationEvent>();
  leases = new Set<string>();
  workers = new Map<string, number>();
  async lease(key: string) {
    if (this.leases.has(key)) return false;
    this.leases.add(key);
    return true;
  }
  async loadRuleState(id: string) {
    return { ...this.states.get(id) };
  }
  async saveRuleState(id: string, state: RuleState) {
    this.states.set(id, { ...state });
  }
  async loadIncident(id: string) {
    return this.incidents.get(id) ?? null;
  }
  async saveIncident(incident: AlertIncident) {
    this.incidents.set(incident.id, incident);
  }
  async commitIncident(
    id: string,
    state: RuleState,
    incident: AlertIncident,
    event: NotificationEvent,
  ) {
    await this.saveRuleState(id, state);
    await this.saveIncident(incident);
    this.events.set(event.id, event);
  }
  async listIncidents() {
    return [...this.incidents.values()];
  }
  async enqueueNotification(event: NotificationEvent) {
    this.events.set(event.id, event);
  }
  async dueNotifications(now: number) {
    return [...this.events.values()].filter(
      (event) => event.status === 'pending' && event.nextRetryAt <= now,
    );
  }
  async listNotifications() {
    return [...this.events.values()];
  }
  async saveNotification(event: NotificationEvent) {
    this.events.set(event.id, event);
  }
  async finishNotification(event: NotificationEvent) {
    this.events.set(event.id, event);
  }
  async claimDelivery() {
    return true;
  }
  async rememberWorker(queue: string, id: string, at: number) {
    this.workers.set(`${queue}:${id}`, at);
  }
  async lastWorkerHeartbeat(queue: string, id: string) {
    return this.workers.get(`${queue}:${id}`) ?? null;
  }
}
const counts = {
  waiting: 0,
  active: 0,
  delayed: 0,
  completed: 0,
  failed: 0,
  paused: 0,
  'waiting-children': 0,
};
const sample = (at: number, failed: number): HistorySample => ({
  queue: 'demo',
  observedAt: at,
  counts: { ...counts, failed },
  paused: false,
  oldestWaitingAgeMs: null,
  completedPerMinute: 0,
  failedPerMinute: 0,
  rateSampleSize: 0,
  source: 'sampled',
});
const rule: AlertRule = {
  id: 'failed-demo',
  queue: 'demo',
  metric: 'failed_count',
  threshold: 2,
  sustainMs: 120_000,
  cooldownMs: 120_000,
  channel: 'webhook',
  enabled: true,
};

test('sustained alert survives engine restart, deduplicates, recovers, and respects cooldown', async () => {
  const store = new MemoryStore();
  const first = new AlertEngine(store, [rule], 60_000);
  await first.evaluate(sample(60_000, 2));
  await first.evaluate(sample(120_000, 2));
  assert.equal(store.events.size, 0);
  const restarted = new AlertEngine(store, [rule], 60_000);
  await restarted.evaluate(sample(180_000, 2));
  await restarted.evaluate(sample(240_000, 2));
  assert.equal(store.events.size, 1);
  await restarted.evaluate(sample(300_000, 0));
  assert.equal(store.events.size, 2);
  assert.equal([...store.incidents.values()][0]?.status, 'resolved');
  await restarted.evaluate(sample(360_000, 2));
  await restarted.evaluate(sample(420_000, 2));
  assert.equal(store.events.size, 2);
  await restarted.evaluate(sample(480_000, 2));
  assert.equal(store.events.size, 3);
});

test('gaps break a sustained window and no activity does not trigger rate alerts', async () => {
  const store = new MemoryStore();
  const engine = new AlertEngine(store, [rule], 60_000);
  await engine.evaluate(sample(60_000, 2));
  await engine.evaluate(sample(180_000, 2));
  await engine.evaluate(sample(240_000, 2));
  assert.equal(store.events.size, 0);
  await engine.evaluate(sample(300_000, 2));
  assert.equal(store.events.size, 1);
  const rateStore = new MemoryStore();
  const rate = new AlertEngine(
    rateStore,
    [{ ...rule, id: 'rate', metric: 'failure_rate', threshold: 1, sustainMs: 0 }],
    60_000,
  );
  await rate.evaluate(sample(60_000, 0));
  assert.equal(rateStore.events.size, 0);
  const heartbeatStore = new MemoryStore();
  const heartbeat = new AlertEngine(
    heartbeatStore,
    [
      {
        ...rule,
        id: 'heartbeat',
        metric: 'heartbeat_gap',
        workerId: 'worker-1',
        threshold: 60_000,
        sustainMs: 0,
      },
    ],
    60_000,
  );
  await heartbeat.evaluate(sample(60_000, 0));
  assert.equal(heartbeatStore.events.size, 0);
  await heartbeat.evaluate(sample(120_000, 0), new Map([['worker-1', 60_000]]));
  assert.equal(heartbeatStore.events.size, 1);
});

test('notification failures are visible and retry with the same event ID', async () => {
  const store = new MemoryStore();
  const engine = new AlertEngine(store, [{ ...rule, sustainMs: 0 }], 60_000);
  await engine.evaluate(sample(60_000, 2));
  let attempts = 0;
  const dispatcher = new NotificationDispatcher(store, [
    {
      channel: 'webhook',
      async send() {
        attempts++;
        if (attempts === 1) throw new Error('network secret');
      },
    },
  ]);
  await dispatcher.dispatch(60_000);
  const [event] = [...store.events.values()];
  assert.equal(event?.status, 'pending');
  assert.equal(event?.lastErrorCode, 'DELIVERY_FAILED');
  await dispatcher.dispatch(event!.nextRetryAt);
  assert.equal(store.events.get(event!.id)?.status, 'sent');
  assert.equal(attempts, 2);
});

test('notification adapters use safe aggregate fields and validate destinations', async () => {
  const store = new MemoryStore();
  await new AlertEngine(store, [{ ...rule, sustainMs: 0, channel: 'email' }], 60_000).evaluate(
    sample(60_000, 2),
  );
  const event = [...store.events.values()][0]!;
  let message = '';
  await createEmailSink('ops@example.com', async (mail) => {
    message = mail.text;
  }).send(event);
  assert.match(message, /Queue: demo/);
  assert.doesNotMatch(message, /payload|password|stacktrace/i);
  assert.throws(() => createWebhookSink('http://example.com/hook'), /HTTPS/);
  assert.throws(() => createEmailSink('invalid', async () => undefined), /recipient/);
});

test('history collection tolerates a failed history store without interrupting queue reads', async () => {
  const queue: ReadOnlyQueue = {
    name: 'demo',
    async getCounts() {
      return counts;
    },
    async isPaused() {
      return false;
    },
    async listJobs() {
      return [];
    },
    async getJob() {
      return null;
    },
    async getJobMetadata() {
      return null;
    },
  };
  const registry = new QueueRegistry([queue]);
  const poller = new SnapshotPoller(registry);
  let attempts = 0;
  const store = {
    collectionIntervalMs: 60_000,
    async append() {
      attempts++;
      throw new Error('history Redis unavailable');
    },
  } as unknown as RedisHistoryStore;
  const collector = new HistoryCollector(registry, poller, store);
  await collector.collect();
  assert.equal(attempts, 1);
  assert.equal((await poller.get('demo')).data.name, 'demo');
  collector.stop();
  poller.stop();
});

test('standalone validates alert and history options before opening owned queues', () => {
  assert.throws(
    () =>
      createStandaloneMonitor({
        redisUrl: 'redis://127.0.0.1:6379',
        queueNames: ['demo'],
        token: 'secret',
        history: { redisUrl: 'redis://127.0.0.1:6379', collectionIntervalMs: 1 },
      }),
    /History interval/,
  );
  assert.throws(
    () =>
      createStandaloneMonitor({
        redisUrl: 'redis://127.0.0.1:6379',
        queueNames: ['demo'],
        token: 'secret',
        alertRules: [{ ...rule, queue: 'unknown' }],
      }),
    /unconfigured queue/,
  );
});

test('history, alerts, and metrics enforce queue authorization and expose no payloads', async () => {
  const queue = (name: string): ReadOnlyQueue => ({
    name,
    async getCounts() {
      return counts;
    },
    async isPaused() {
      return false;
    },
    async listJobs() {
      return [];
    },
    async getJob() {
      return null;
    },
    async getJobMetadata() {
      return null;
    },
  });
  const store = new MemoryStore();
  await store.saveIncident({
    id: 'hidden:1',
    ruleId: 'hidden',
    queue: 'hidden',
    metric: 'backlog',
    openedAt: 1,
    status: 'open',
    lastValue: 2,
  });
  await store.saveIncident({
    id: 'demo:1',
    ruleId: 'demo',
    queue: 'demo',
    metric: 'backlog',
    openedAt: 1,
    status: 'open',
    lastValue: 2,
  });
  const handler = createHttpHandler({
    registry: new QueueRegistry([queue('demo'), queue('hidden')]),
    token: 'secret',
    authorize: (_req, target) => target.queueName === 'demo',
    alertStore: store,
    alertRules: [
      { ...rule, queue: 'demo' },
      { ...rule, id: 'hidden', queue: 'hidden' },
    ],
    history: {
      collectionIntervalMs: 60_000,
      retentionMs: 86_400_000,
      async read(name) {
        return {
          from: 0,
          to: 1,
          stepMs: 60_000,
          collectionIntervalMs: 60_000,
          retentionMs: 86_400_000,
          slots: [{ at: 0, gap: false, sample: sample(0, name === 'demo' ? 1 : 99) }],
          source: 'sampled' as const,
          partial: false,
          warnings: [],
        };
      },
    },
  });
  const call = async (url: string, auth = 'Bearer secret') => {
    let status = 0;
    let body = '';
    const req = { method: 'GET', url, headers: { authorization: auth } } as IncomingMessage;
    const res = Object.assign(new EventEmitter(), {
      writeHead(code: number) {
        status = code;
        return this;
      },
      end(chunk?: string) {
        body += chunk ?? '';
        return this;
      },
    }) as unknown as ServerResponse;
    await handler(req, res);
    return { status, body };
  };
  assert.equal((await call('/api/v1/queues/demo/history', '')).status, 401);
  assert.equal((await call('/api/v1/queues/hidden/history')).status, 403);
  assert.equal((await call('/api/v1/queues/demo/history?range=bad')).status, 400);
  assert.equal((await call('/api/v1/queues/demo/history')).status, 200);
  const incidents = await call('/api/v1/alerts/incidents');
  assert.equal(incidents.status, 200);
  assert.doesNotMatch(incidents.body, /hidden/);
  const metrics = await call('/api/v1/metrics');
  assert.equal(metrics.status, 200);
  assert.match(metrics.body, /queuelens_queue_jobs/);
  assert.doesNotMatch(metrics.body, /hidden/);
});
