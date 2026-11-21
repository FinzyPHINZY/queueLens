import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { resolve } from 'node:path';
import { test } from 'node:test';
import { Queue } from 'bullmq';
import express from 'express';
import { redisOptions } from '../../packages/bullmq-adapter/src/connection.js';
import { BullMqQueueAdapter } from '../../packages/bullmq-adapter/src/adapter.js';
import { QueueRegistry } from '../../packages/core/src/monitor.js';
import { SnapshotPoller } from '../../packages/core/src/poller.js';
import { createExpressMonitor, createStandaloneMonitor } from '../../packages/monitor/src/index.js';

test('standalone serves a real queue and dashboard with protected job data', async () => {
  const name = `standalone-test-${process.pid}-${Date.now()}`;
  const redisUrl = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379';
  const queue = new Queue(name, { connection: redisOptions(redisUrl) });
  await queue.waitUntilReady();
  await queue.add('sample', { password: 'integration-secret' }, { jobId: 'one' });
  const monitor = createStandaloneMonitor({
    redisUrl,
    queueNames: [name],
    token: 'test-token',
    dashboardDir: resolve('apps/dashboard/dist'),
  });
  const server = createServer(monitor.handler);
  try {
    await new Promise<void>((ready) => server.listen(0, '127.0.0.1', ready));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing test address');
    const base = `http://127.0.0.1:${address.port}`;
    const html = await (await fetch(base)).text();
    assert.match(html, /QueueLens/);
    const asset = html.match(/\.\/assets\/[^"']+\.js/)?.[0];
    assert.ok(asset);
    assert.equal((await fetch(new URL(asset, `${base}/`))).status, 200);
    assert.equal((await fetch(`${base}/api/v1/queues`)).status, 401);
    const headers = { authorization: 'Bearer test-token' };
    const directory = await (await fetch(`${base}/api/v1/queues`, { headers })).json();
    assert.equal(directory.data[0].counts.waiting, 1);
    const detail = await (
      await fetch(`${base}/api/v1/queues/${name}/jobs/one`, { headers })
    ).text();
    assert.equal(JSON.parse(detail).data.state, 'waiting');
    assert.doesNotMatch(detail, /integration-secret/);
  } finally {
    await new Promise<void>((closed) => server.close(() => closed()));
    await monitor.close();
    await queue.obliterate({ force: true });
    await queue.close();
  }
});

test('Express mount serves assets and authorizes queue reads with a host session', async () => {
  const name = `express-test-${process.pid}-${Date.now()}`;
  const queue = new Queue(name, {
    connection: redisOptions(process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379'),
  });
  await queue.waitUntilReady();
  await queue.add('sample', { password: 'hidden' });
  const monitor = createExpressMonitor({
    queues: [queue],
    dashboardDir: resolve('apps/dashboard/dist'),
    authorize: async (request) => request.headers.cookie === 'session=allowed',
  });
  const app = express();
  app.use('/monitor', monitor.handler);
  const server = createServer(app);
  try {
    await new Promise<void>((ready) => server.listen(0, '127.0.0.1', ready));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing test address');
    const base = `http://127.0.0.1:${address.port}`;
    const redirect = await fetch(`${base}/monitor`, { redirect: 'manual' });
    assert.equal(redirect.status, 308);
    assert.equal(redirect.headers.get('location'), '/monitor/');
    const html = await (await fetch(`${base}/monitor/`)).text();
    const asset = html.match(/\.\/assets\/[^"']+\.js/)?.[0];
    assert.ok(asset);
    assert.equal((await fetch(new URL(asset, `${base}/monitor/`))).status, 200);
    assert.equal((await fetch(`${base}/monitor/api/v1/queues`)).status, 403);
    const authorized = await fetch(`${base}/monitor/api/v1/queues`, {
      headers: { cookie: 'session=allowed' },
    });
    assert.equal(authorized.status, 200);
    assert.equal((await authorized.json()).data[0].counts.waiting, 1);
  } finally {
    await new Promise<void>((closed) => server.close(() => closed()));
    await monitor.close();
    await queue.obliterate({ force: true });
    await queue.close();
  }
});

test('real Redis disconnect returns stale data and reconnect restores fresh reads', async () => {
  const name = `reconnect-test-${process.pid}-${Date.now()}`;
  const queue = new Queue(name, {
    connection: redisOptions(process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379'),
  });
  const poller = new SnapshotPoller(new QueueRegistry([new BullMqQueueAdapter(queue)]), {
    intervalMs: 1_000,
    queryTimeoutMs: 300,
  });
  const client = await queue.client;
  try {
    await queue.waitUntilReady();
    assert.equal((await poller.get(name)).meta.partial, false);
    client.disconnect();
    await new Promise((resolve) => setTimeout(resolve, 1_010));
    const stale = await poller.get(name);
    assert.equal(stale.meta.partial, true);
    await client.connect();
    await new Promise((resolve) => setTimeout(resolve, 1_010));
    assert.equal((await poller.get(name)).meta.partial, false);
  } finally {
    poller.stop();
    if (client.status === 'end') await client.connect();
    await queue.obliterate({ force: true });
    await queue.close();
  }
});
