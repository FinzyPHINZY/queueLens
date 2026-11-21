import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { resolve } from 'node:path';
import { test } from 'node:test';
import { Queue } from 'bullmq';
import { redisOptions } from '../../packages/bullmq-adapter/src/connection.js';
import { createStandaloneMonitor } from '../../packages/monitor/src/index.js';

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
