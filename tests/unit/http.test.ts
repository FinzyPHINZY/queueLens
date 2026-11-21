import assert from 'node:assert/strict';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { test } from 'node:test';
import { QueueRegistry, type ReadOnlyQueue } from '../../packages/core/src/monitor.js';
import { createHttpHandler } from '../../packages/server/src/http.js';

const queue: ReadOnlyQueue = {
  name: 'demo',
  async getCounts() {
    return {
      waiting: 1,
      active: 0,
      delayed: 0,
      completed: 0,
      failed: 0,
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
  async getJobMetadata(id) {
    return id === 'known'
      ? { id, name: 'sample', state: 'waiting', attemptsMade: 0, timestamp: 1 }
      : null;
  },
};

const handler = createHttpHandler({ registry: new QueueRegistry([queue]), token: 'secret' });

async function request(method: string, url: string, authorization?: string) {
  let status = 0;
  let body = '';
  const req = { method, url, headers: { authorization } } as IncomingMessage;
  const res = {
    writeHead(code: number) {
      status = code;
      return this;
    },
    end(chunk: string) {
      body = chunk;
      return this;
    },
  } as ServerResponse;
  await handler(req, res);
  return { status, body: JSON.parse(body) };
}

test('queue data requires a bearer token', async () => {
  const denied = await request('GET', '/api/v1/queues');
  assert.equal(denied.status, 401);
  assert.equal(denied.body.error.code, 'UNAUTHORIZED');

  const wrong = await request('GET', '/api/v1/queues', 'Bearer wrong');
  assert.equal(wrong.status, 401);

  const allowed = await request('GET', '/api/v1/queues', 'Bearer secret');
  assert.equal(allowed.status, 200);
  assert.equal(allowed.body.data[0].name, 'demo');
  assert.equal(allowed.body.meta.source, 'bullmq');
  assert.equal(typeof allowed.body.meta.observedAt, 'string');
});

test('unknown jobs use a stable error and mutations are rejected', async () => {
  const missing = await request('GET', '/api/v1/queues/demo/jobs/missing', 'Bearer secret');
  assert.equal(missing.status, 404);
  assert.equal(missing.body.error.code, 'JOB_NOT_FOUND');

  const mutation = await request('POST', '/api/v1/queues/demo/jobs/known', 'Bearer secret');
  assert.equal(mutation.status, 405);

  const malformed = await request('GET', '/api/v1/queues/%ZZ', 'Bearer secret');
  assert.equal(malformed.status, 400);
  assert.equal(malformed.body.error.code, 'INVALID_REQUEST');
});
