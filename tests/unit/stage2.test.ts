import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { test } from 'node:test';
import type { Queue, Worker } from 'bullmq';
import { BullMqQueueAdapter } from '../../packages/bullmq-adapter/src/adapter.js';
import { sampleQueueDiagnostics } from '../../packages/core/src/diagnostics.js';
import { QueueRegistry, type RawJob, type ReadOnlyQueue } from '../../packages/core/src/monitor.js';
import { SnapshotPoller } from '../../packages/core/src/poller.js';
import { ReadBudgetExceededError } from '../../packages/core/src/read-budget.js';
import { instrumentWorker } from '../../packages/monitor/src/worker-telemetry.js';
import { createStandaloneMonitor } from '../../packages/monitor/src/index.js';
import { createHttpHandler } from '../../packages/server/src/http.js';

const counts = {
  waiting: 1,
  active: 0,
  delayed: 0,
  completed: 1,
  failed: 1,
  paused: 0,
  'waiting-children': 0,
};
function fakeQueue(name: string): ReadOnlyQueue {
  return {
    name,
    async getCounts() {
      return counts;
    },
    async isPaused() {
      return false;
    },
    async getOldestWaitingAt() {
      return Date.now() - 10_000;
    },
    async listJobs({ state }) {
      const now = Date.now();
      const base: RawJob = {
        id: `${state}-1`,
        name: 'task',
        state,
        attemptsMade: state === 'failed' ? 2 : 1,
        timestamp: now - 1_000,
        processedOn: now - 800,
        finishedOn: now - 100,
        data: { secret: 'never-return-this' },
        returnValue: undefined,
        stacktrace: [],
        failedReason: state === 'failed' ? 'Timeout password=never-return-this' : undefined,
      };
      return [base];
    },
    async getJob() {
      return null;
    },
    async getJobMetadata() {
      return null;
    },
    async listWorkers() {
      return [];
    },
  };
}

test('diagnostics are bounded and disclose only aggregate, sampled values', async () => {
  const queue = fakeQueue('demo');
  const data = await sampleQueueDiagnostics(queue, (operation) => operation(), []);
  assert.equal(data.source, 'sampled');
  assert.equal(data.completed, 1);
  assert.equal(data.failed, 1);
  assert.equal(data.retried, 1);
  assert.equal(data.processing.sampleSize, 2);
  assert.equal(data.processing.p95Ms, null);
  assert.equal(data.errorCategories.timeout, 1);
  assert.equal(data.outcomeTrend.length, 5);
  assert.equal(data.outcomeTrend.at(-1)?.failed, 1);
  assert.equal(data.outcomeTrend.at(-1)?.retried, 1);
  assert.doesNotMatch(JSON.stringify(data), /never-return-this/);
});

test('per-queue read budget prevents one queue from monopolizing reads', async () => {
  assert.throws(
    () =>
      createStandaloneMonitor({
        redisUrl: 'redis://localhost:6379',
        queueNames: ['demo'],
        token: 'secret',
        polling: { maxReadsPerQueue: 0 },
      }),
    /MONITOR_MAX_READS_PER_QUEUE/,
  );
  const poller = new SnapshotPoller(new QueueRegistry([fakeQueue('demo')]), {
    maxReadsPerQueue: 1,
  });
  let release!: () => void;
  const first = poller.runQueueRead(
    'demo',
    () =>
      new Promise<void>((resolve) => {
        release = resolve;
      }),
  );
  await Promise.resolve();
  assert.throws(() => poller.runQueueRead('demo', async () => undefined), ReadBudgetExceededError);
  release();
  await first;
  poller.stop();
});

test('worker telemetry labels only fresh instrumented heartbeats online', async () => {
  const values = new Map<string, string>();
  const members = new Map<string, number>();
  const redis = {
    multi() {
      const commands: (() => void)[] = [];
      const chain = {
        set(key: string, value: string) {
          commands.push(() => values.set(key, value));
          return chain;
        },
        zadd(_key: string, score: number, id: string) {
          commands.push(() => members.set(id, score));
          return chain;
        },
        zremrangebyscore() {
          return chain;
        },
        del(key: string) {
          commands.push(() => values.delete(key));
          return chain;
        },
        zrem(_key: string, id: string) {
          commands.push(() => members.delete(id));
          return chain;
        },
        async exec() {
          commands.forEach((command) => command());
          return [];
        },
      };
      return chain;
    },
    async zrevrangebyscore() {
      return [...members.keys()];
    },
    async mget(keys: string[]) {
      return keys.map((key) => values.get(key) ?? null);
    },
  };
  const worker = Object.assign(new EventEmitter(), {
    name: 'demo',
    client: Promise.resolve(redis),
  }) as unknown as Worker;
  const telemetry = instrumentWorker(worker, {
    id: 'worker-1',
    concurrency: 2,
    heartbeatTtlMs: 2_000,
    heartbeatIntervalMs: 1_000,
  });
  await telemetry.heartbeat();
  const adapter = new BullMqQueueAdapter({
    name: 'demo',
    client: Promise.resolve(redis),
  } as unknown as Queue);
  const [online] = await adapter.listWorkers();
  assert.equal(online?.state, 'online');
  worker.emit('stalled', 'job-1', 'active');
  await telemetry.heartbeat();
  const [signaled] = await adapter.listWorkers();
  assert.equal(signaled?.stalledSignals, 1);
  assert.equal(signaled?.activeJobIds.length, 0);
  const key = 'queuelens:workers:demo:worker-1';
  values.set(key, JSON.stringify({ ...signaled, unexpectedSecret: 'never-return-this' }));
  assert.doesNotMatch(JSON.stringify(await adapter.listWorkers()), /never-return-this/);
  values.set(key, JSON.stringify({ ...signaled, lastHeartbeat: Date.now() - 3_000 }));
  const [stale] = await adapter.listWorkers();
  assert.equal(stale?.state, 'stale');
  await telemetry.heartbeat();
  assert.equal((await adapter.listWorkers())[0]?.state, 'online');
  await telemetry.close();
  assert.equal((await adapter.listWorkers()).length, 0);
});

test('worker route enforces per-queue authorization and SSE streams authorized snapshots', async () => {
  const allowed = {
    ...fakeQueue('allowed'),
    async getFlow() {
      return {
        parent: { id: 'secret-parent', queueKey: 'bull:hidden' },
        childKeys: ['bull:allowed:child-1', 'bull:hidden:secret-child'],
        partial: false,
      };
    },
  };
  const handler = createHttpHandler({
    registry: new QueueRegistry([allowed, fakeQueue('hidden')]),
    token: 'secret',
    authorize: (_request, target) => target.queueName === 'allowed',
  });
  const call = async (url: string, authorization?: string) => {
    let status = 0;
    let contents = '';
    const request = { method: 'GET', url, headers: { authorization } } as IncomingMessage;
    const response = Object.assign(new EventEmitter(), {
      writeHead(code: number) {
        status = code;
        return this;
      },
      write(chunk: string) {
        contents += chunk;
        return true;
      },
      end(chunk?: string) {
        contents += chunk ?? '';
        return this;
      },
    }) as unknown as ServerResponse;
    await handler(request, response);
    await new Promise((resolve) => setImmediate(resolve));
    return { status, contents, response };
  };
  assert.equal((await call('/api/v1/workers')).status, 401);
  const workers = await call('/api/v1/workers', 'Bearer secret');
  assert.equal(workers.status, 200);
  assert.equal(JSON.parse(workers.contents).data.uninstrumentedLiveness, 'unknown');
  const flow = await call('/api/v1/queues/allowed/jobs/parent/flow', 'Bearer secret');
  assert.equal(flow.status, 200);
  assert.deepEqual(JSON.parse(flow.contents).data.children, [{ queue: 'allowed', id: 'child-1' }]);
  assert.equal(JSON.parse(flow.contents).data.parent, null);
  assert.equal(JSON.parse(flow.contents).meta.partial, true);
  assert.doesNotMatch(flow.contents, /secret-parent|secret-child/);
  const stream = await call('/api/v1/events', 'Bearer secret');
  assert.equal(stream.status, 200);
  assert.match(stream.contents, /retry: 3000/);
  assert.match(stream.contents, /"name":"allowed"/);
  assert.doesNotMatch(stream.contents, /"name":"hidden"/);
  stream.response.emit('close');
});
