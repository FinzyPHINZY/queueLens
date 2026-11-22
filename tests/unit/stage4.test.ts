import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { Readable } from 'node:stream';
import { test } from 'node:test';
import type { Job, Queue } from 'bullmq';
import type {
  AuditEvent,
  ManagementPreviewRecord,
  ManagementResult,
  ManagementStore,
  StoredOperation,
} from '../../packages/core/src/management.js';
import { QueueRegistry, type ReadOnlyQueue } from '../../packages/core/src/monitor.js';
import { ManagementService, ManagementError } from '../../packages/monitor/src/management.js';
import { createHttpHandler } from '../../packages/server/src/http.js';

class MemoryStore implements ManagementStore {
  operations = new Map<string, StoredOperation>();
  previews = new Map<string, ManagementPreviewRecord>();
  events: AuditEvent[] = [];
  failIntent = false;
  failResult = false;
  lock = false;
  async savePreview(token: string, preview: ManagementPreviewRecord) {
    this.previews.set(token, preview);
  }
  async loadPreview(token: string) {
    return this.previews.get(token) ?? null;
  }
  async reserve(
    key: string,
    fingerprint: string,
    intent: AuditEvent,
  ): Promise<'new' | StoredOperation> {
    if (this.failIntent) throw new Error('audit offline');
    const existing = this.operations.get(key);
    if (existing) return existing;
    this.operations.set(key, { fingerprint, status: 'pending' });
    this.events.push(intent);
    return 'new';
  }
  async getOperation(key: string) {
    return this.operations.get(key) ?? null;
  }
  async finish(key: string, operation: StoredOperation, event: AuditEvent) {
    if (this.failResult) throw new Error('audit offline');
    this.operations.set(key, operation);
    this.events.push(event);
  }
  async recent(limit: number) {
    return [...this.events].reverse().slice(0, limit);
  }
  async countAttempt() {
    return true;
  }
  async acquireQueue() {
    if (this.lock) return false;
    this.lock = true;
    return true;
  }
  async renewQueue() {
    return this.lock;
  }
  async releaseQueue() {
    this.lock = false;
  }
  async requestCancellation() {
    return;
  }
  async cancellationRequested() {
    return false;
  }
  async close() {
    return;
  }
}

function fakeQueue() {
  let state = 'failed';
  let paused = false;
  let retries = 0;
  const job = {
    id: 'failed-1',
    parent: undefined,
    opts: {},
    repeatJobKey: undefined,
    finishedOn: Date.now() - 3_600_000,
    async getState() {
      return state;
    },
    async retry() {
      retries++;
      state = 'waiting';
    },
    async promote() {
      state = 'waiting';
    },
    async remove() {
      state = 'unknown';
    },
    async getDependenciesCount() {
      return { processed: 0, unprocessed: 0 };
    },
  } as unknown as Job;
  const queue = {
    name: 'demo',
    async getJob(id: string) {
      return id === 'failed-1' ? job : null;
    },
    async getJobs() {
      return [job];
    },
    async isPaused() {
      return paused;
    },
    async pause() {
      paused = true;
    },
    async resume() {
      paused = false;
    },
  } as unknown as Queue;
  return {
    queue,
    get retries() {
      return retries;
    },
    setState(next: string) {
      state = next;
    },
    get paused() {
      return paused;
    },
  };
}

const adminKey = 'request-0001';

test('audit intent precedes mutation, and idempotent replay never repeats a retry', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  const service = new ManagementService([fake.queue], store);
  const first = await service.execute('alice', 'retry', 'demo', { jobId: 'failed-1' }, adminKey);
  assert.equal(fake.retries, 1);
  assert.equal(store.events[0]?.phase, 'intent');
  assert.equal(store.events[1]?.phase, 'result');
  assert.equal(
    (await service.execute('alice', 'retry', 'demo', { jobId: 'failed-1' }, adminKey)).operationId,
    first.operationId,
  );
  assert.equal(fake.retries, 1);
  await assert.rejects(
    service.execute('alice', 'pause', 'demo', {}, adminKey),
    (cause: unknown) => cause instanceof ManagementError && cause.code === 'IDEMPOTENCY_CONFLICT',
  );
});

test('audit outages fail closed and state changes are audited', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  const service = new ManagementService([fake.queue], store);
  store.failIntent = true;
  await assert.rejects(service.execute('alice', 'retry', 'demo', { jobId: 'failed-1' }, adminKey));
  assert.equal(fake.retries, 0);
  store.failIntent = false;
  fake.setState('active');
  await assert.rejects(
    service.execute('alice', 'retry', 'demo', { jobId: 'failed-1' }, 'request-0002'),
    (cause: unknown) => cause instanceof ManagementError && cause.code === 'STATE_CHANGED',
  );
  assert.equal(store.events.at(-1)?.outcome, 'rejected');
  assert.equal(fake.retries, 0);
});

test('result audit failure returns uncertain outcome after mutation', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  store.failResult = true;
  const service = new ManagementService([fake.queue], store);
  await assert.rejects(
    service.execute('alice', 'retry', 'demo', { jobId: 'failed-1' }, adminKey),
    (cause: unknown) =>
      cause instanceof ManagementError && cause.code === 'MANAGEMENT_OUTCOME_UNCERTAIN',
  );
  assert.equal(fake.retries, 1);
  assert.equal(store.events.length, 1);
});

test('queue locks reject concurrent actions and record the rejection', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  store.lock = true;
  const service = new ManagementService([fake.queue], store);
  await assert.rejects(
    service.execute('alice', 'retry', 'demo', { jobId: 'failed-1' }, 'locked-0001'),
    (cause: unknown) => cause instanceof ManagementError && cause.code === 'QUEUE_BUSY',
  );
  assert.equal(fake.retries, 0);
  assert.equal(store.events.at(-1)?.outcome, 'rejected');
});

test('shared previews bind actor and scope, and bulk actions report each result', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  const first = new ManagementService([fake.queue], store);
  const second = new ManagementService([fake.queue], store);
  const preview = await first.preview('alice', 'demo', 'bulk-retry', {
    jobIds: ['failed-1', 'missing'],
  });
  await assert.rejects(
    second.execute(
      'bob',
      'bulk-retry',
      'demo',
      { jobIds: preview.jobIds, previewToken: preview.previewToken },
      'bulk-0001',
    ),
    (cause: unknown) => cause instanceof ManagementError && cause.code === 'PREVIEW_MISMATCH',
  );
  const result = await second.execute(
    'alice',
    'bulk-retry',
    'demo',
    { jobIds: preview.jobIds, previewToken: preview.previewToken },
    'bulk-0001',
  );
  assert.equal(result.status, 'partial');
  assert.deepEqual(
    result.items?.map((item) => item.status),
    ['succeeded', 'rejected'],
  );
  assert.equal(fake.retries, 1);
  store.previews.delete(preview.previewToken);
  assert.equal(
    (
      await second.execute(
        'alice',
        'bulk-retry',
        'demo',
        { jobIds: preview.jobIds, previewToken: preview.previewToken },
        'bulk-0001',
      )
    ).operationId,
    result.operationId,
  );
  assert.equal(fake.retries, 1);
});

test('clean requires preview and rechecks terminal age before removing', async () => {
  const fake = fakeQueue();
  fake.setState('completed');
  const store = new MemoryStore();
  const service = new ManagementService([fake.queue], store);
  const preview = await service.preview('alice', 'demo', 'clean', {
    state: 'completed',
    olderThanMs: 60_000,
    limit: 1,
  });
  assert.deepEqual(preview.jobIds, ['failed-1']);
  const result = await service.execute(
    'alice',
    'clean',
    'demo',
    {
      state: 'completed',
      olderThanMs: 60_000,
      jobIds: preview.jobIds,
      previewToken: preview.previewToken,
    },
    'clean-0001',
  );
  assert.equal(result.items?.[0]?.status, 'succeeded');
  assert.equal(await fake.queue.getJob('failed-1').then((job) => job?.getState()), 'unknown');
});

test('management API denies read token and exposes only admin-authorized audit records', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  const service = new ManagementService([fake.queue], store);
  const readQueue: ReadOnlyQueue = {
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
  const registry = new QueueRegistry([readQueue, { ...readQueue, name: 'other' }]);
  const adminToken = 'admin-secret-that-is-distinct-and-long';
  const handler = createHttpHandler({
    registry,
    token: 'read-secret',
    management: { service, adminToken, actorId: 'alice', adminQueues: ['demo'], bearerOnly: true },
  });
  const disabled = createHttpHandler({ registry, token: 'read-secret' });
  const call = async (url: string, token: string, target = handler) => {
    let status = 0;
    let body = '';
    const source = Readable.from(['{"jobId":"failed-1"}']);
    const req = Object.assign(source, {
      method: 'POST',
      url,
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
        'idempotency-key': adminKey,
      },
    }) as unknown as IncomingMessage;
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
    await target(req, res);
    return { status, body };
  };
  assert.equal(
    (await call('/api/v1/admin/queues/demo/actions/retry', adminToken, disabled)).status,
    404,
  );
  assert.equal((await call('/api/v1/admin/queues/demo/actions/retry', 'read-secret')).status, 403);
  assert.equal((await call('/api/v1/admin/queues/other/actions/retry', adminToken)).status, 403);
  const result = await call('/api/v1/admin/queues/demo/actions/retry', adminToken);
  assert.equal(result.status, 200);
  assert.equal((JSON.parse(result.body).data as ManagementResult).status, 'succeeded');
  assert.equal(fake.retries, 1);
  assert.doesNotMatch(JSON.stringify(store.events), /admin-secret|read-secret/);
});

test('cookie-authenticated management rejects cross-origin and missing CSRF proof', async () => {
  const fake = fakeQueue();
  const store = new MemoryStore();
  const readQueue: ReadOnlyQueue = {
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
  const handler = createHttpHandler({
    registry: new QueueRegistry([readQueue]),
    authorize: async () => true,
    management: {
      service: new ManagementService([fake.queue], store),
      authorize: async () => ({ id: 'alice' }),
      verifyCsrf: (req) => req.headers['x-csrf-token'] === 'valid',
    },
  });
  {
    let status = 0;
    const request = Object.assign(Readable.from([]), {
      method: 'GET',
      url: '/api/v1/admin/capabilities',
      headers: { host: 'queuelens.test' },
    }) as unknown as IncomingMessage;
    const response = Object.assign(new EventEmitter(), {
      writeHead(code: number) {
        status = code;
        return this;
      },
      end() {
        return this;
      },
    }) as unknown as ServerResponse;
    await handler(request, response);
    assert.equal(status, 200);
  }
  const call = async (origin: string, csrf: string) => {
    let status = 0;
    const request = Object.assign(Readable.from(['{"jobId":"failed-1"}']), {
      method: 'POST',
      url: '/api/v1/admin/queues/demo/actions/retry',
      headers: {
        host: 'queuelens.test',
        origin,
        'x-csrf-token': csrf,
        'content-type': 'application/json',
        'idempotency-key': 'csrf-0001',
      },
    }) as unknown as IncomingMessage;
    const response = Object.assign(new EventEmitter(), {
      writeHead(code: number) {
        status = code;
        return this;
      },
      end() {
        return this;
      },
    }) as unknown as ServerResponse;
    await handler(request, response);
    return status;
  };
  assert.equal(await call('https://evil.example', 'valid'), 403);
  assert.equal(await call('http://queuelens.test', 'wrong'), 403);
  assert.equal(fake.retries, 0);
  assert.equal(await call('http://queuelens.test', 'valid'), 200);
  assert.equal(fake.retries, 1);
});
