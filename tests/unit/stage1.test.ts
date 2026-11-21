import assert from 'node:assert/strict';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { test } from 'node:test';
import { QueueRegistry, type RawJob, type ReadOnlyQueue } from '../../packages/core/src/monitor.js';
import { SnapshotPoller } from '../../packages/core/src/poller.js';
import { ReadBudget, ReadBudgetExceededError } from '../../packages/core/src/read-budget.js';
import { safeJobDetail } from '../../packages/core/src/redaction.js';
import { createHttpHandler } from '../../packages/server/src/http.js';
import { createStandaloneMonitor } from '../../packages/monitor/src/index.js';

const job: RawJob = {
  id: 'one',
  name: 'email',
  state: 'failed',
  attemptsMade: 2,
  timestamp: 1,
  progress: { secret: 'progress-secret' },
  data: { user: { id: 12, password: 'nested-secret' }, token: 'top-secret' },
  returnValue: { token: 'returned-secret' },
  failedReason: 'redis://user:pass@host:6379 token=error-secret',
  stacktrace: ['Authorization: Bearer stack-secret'],
};

const queue: ReadOnlyQueue = {
  name: 'demo',
  async getCounts() {
    return {
      waiting: 0,
      active: 0,
      delayed: 0,
      completed: 0,
      failed: 1,
      paused: 0,
      'waiting-children': 0,
    };
  },
  async isPaused() {
    return false;
  },
  async listJobs({ offset, limit }) {
    return [job].slice(offset, offset + limit);
  },
  async getJob(id) {
    return id === 'one' ? job : null;
  },
  async getJobMetadata(id) {
    return id === 'one' ? job : null;
  },
};

async function call(
  handler: ReturnType<typeof createHttpHandler>,
  url: string,
  authorization = 'Bearer secret',
) {
  let status = 0;
  let result = '';
  const request = { method: 'GET', url, headers: { authorization } } as IncomingMessage;
  const response = {
    writeHead(code: number) {
      status = code;
      return this;
    },
    end(value: string) {
      result = value;
      return this;
    },
  } as ServerResponse;
  await handler(request, response);
  return { status, body: JSON.parse(result) };
}

test('nested allowlists, redaction and failure sanitizing protect response fields', () => {
  const hidden = JSON.stringify(safeJobDetail(job));
  assert.doesNotMatch(
    hidden,
    /nested-secret|top-secret|returned-secret|progress-secret|error-secret/,
  );
  const visible = JSON.stringify(
    safeJobDetail(job, {
      allowedPaths: ['/user/id', '/user/password'],
      redactPaths: ['/user/password'],
      showFailureDetails: true,
    }),
  );
  assert.match(visible, /"id":12/);
  assert.match(visible, /REDACTED/);
  assert.doesNotMatch(
    visible,
    /nested-secret|top-secret|returned-secret|progress-secret|error-secret|stack-secret|user:pass/,
  );
});

test('authorization, page caps and exact job lookup are enforced', async () => {
  const handler = createHttpHandler({
    registry: new QueueRegistry([queue]),
    token: 'secret',
    authorize: (_request, target) => target.permission === 'queue:read',
  });
  const directory = await call(handler, '/api/v1/queues');
  assert.equal(directory.status, 200);
  const deniedDirectory = createHttpHandler({
    registry: new QueueRegistry([queue]),
    authorize: async () => false,
  });
  assert.equal((await call(deniedDirectory, '/api/v1/queues', '')).status, 403);
  assert.equal((await call(handler, '/api/v1/queues/demo/jobs/one')).status, 403);
  const openHandler = createHttpHandler({ registry: new QueueRegistry([queue]), token: 'secret' });
  assert.equal((await call(openHandler, '/api/v1/queues/demo/jobs?limit=101')).status, 400);
  assert.equal((await call(openHandler, '/api/v1/queues/demo/jobs?offset=10001')).status, 400);
  assert.equal((await call(openHandler, '/api/v1/queues/demo/jobs?state=unknown')).status, 400);
  const page = await call(openHandler, '/api/v1/queues/demo/jobs?state=failed&limit=1');
  assert.equal(page.body.success, true);
  assert.equal(page.body.data.jobs[0].id, 'one');
  assert.doesNotMatch(JSON.stringify(page.body), /progress-secret|nested-secret/);
  const missing = await call(openHandler, '/api/v1/queues/demo/jobs/removed');
  assert.equal(missing.body.error.code, 'JOB_NOT_FOUND');
});

test('poller returns stale last-known data after a queue read fails', async () => {
  let fail = false;
  const unstable = {
    ...queue,
    async getCounts() {
      if (fail) throw new Error('Redis down');
      return queue.getCounts();
    },
  };
  const poller = new SnapshotPoller(new QueueRegistry([unstable]), { intervalMs: 1_000 });
  await poller.get('demo');
  fail = true;
  await new Promise((resolve) => setTimeout(resolve, 1_010));
  const stale = await poller.get('demo');
  assert.equal(stale.meta.partial, true);
  assert.equal(stale.data.counts.failed, 1);
  const handler = createHttpHandler({
    registry: new QueueRegistry([unstable]),
    token: 'secret',
    poller,
  });
  const readiness = await call(handler, '/api/v1/ready');
  assert.equal(readiness.status, 503);
  assert.equal(readiness.body.success, false);
  assert.equal(readiness.body.data, null);
  assert.equal(readiness.body.meta.partial, true);
  assert.equal(readiness.body.error.code, 'REDIS_UNAVAILABLE');
  poller.stop();
});

test('read budget rejects work beyond concurrent limit', async () => {
  const budget = new ReadBudget(1, 1_000);
  let release!: () => void;
  const first = budget.run(
    () =>
      new Promise<void>((resolve) => {
        release = resolve;
      }),
  );
  await Promise.resolve();
  assert.throws(() => budget.run(async () => undefined), ReadBudgetExceededError);
  release();
  await first;
});

test('invalid configuration fails before opening owned Redis queues', () => {
  assert.throws(
    () =>
      createStandaloneMonitor({
        redisUrl: 'redis://localhost:6379',
        queueNames: ['a', 'a'],
        token: 'secret',
      }),
    /unique/,
  );
  assert.throws(
    () =>
      createStandaloneMonitor({
        redisUrl: 'redis://localhost:6379',
        queueNames: ['a'],
        token: 'secret',
        polling: { maxConcurrentReads: 0 },
      }),
    /maxConcurrentReads/,
  );
  assert.throws(
    () =>
      createHttpHandler({
        registry: new QueueRegistry([queue]),
        token: 'secret',
        redaction: { allowedPaths: ['user/id'] },
      }),
    /JSON Pointer/,
  );
  assert.throws(
    () =>
      createHttpHandler({
        registry: new QueueRegistry([queue]),
        token: 'secret',
        redaction: { maxValueBytes: 10_000_000 },
      }),
    /maxValueBytes/,
  );
});

test('operational logs omit Redis errors and request identifiers', async () => {
  const entries: unknown[] = [];
  const broken = {
    ...queue,
    async getJob() {
      throw new Error('redis://user:secret@host password=secret');
    },
  };
  const handler = createHttpHandler({
    registry: new QueueRegistry([broken]),
    token: 'secret',
    log: (entry) => entries.push(entry),
  });
  const response = await call(handler, '/api/v1/queues/demo/jobs/private-id');
  assert.equal(response.status, 503);
  const output = JSON.stringify(entries);
  assert.match(output, /read_failed/);
  assert.doesNotMatch(output, /secret@host|password=secret|private-id/);
});
