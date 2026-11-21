import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { test } from 'node:test';
import { Queue, Worker } from 'bullmq';
import { BullMqQueueAdapter } from '../../packages/bullmq-adapter/src/adapter.js';
import { redisOptions } from '../../packages/bullmq-adapter/src/connection.js';
import { QueueRegistry } from '../../packages/core/src/monitor.js';
import { createHttpHandler } from '../../packages/server/src/http.js';

test('reads real BullMQ jobs without mutating or closing a caller-owned queue', async () => {
  const name = `monitor-test-${process.pid}-${Date.now()}`;
  const connection = redisOptions(process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379');
  const queue = new Queue(name, { connection });
  const second = new Queue(`${name}-second`, { connection });
  const worker = new Worker(
    name,
    async (job) => {
      if (job.name === 'fail') throw new Error('synthetic failure');
      return 'done';
    },
    { connection: { ...connection, maxRetriesPerRequest: null } },
  );
  const registry = new QueueRegistry([
    new BullMqQueueAdapter(queue),
    new BullMqQueueAdapter(second),
  ]);
  const server = createServer(
    createHttpHandler({ registry, token: 'test-token', redaction: { showFailureDetails: true } }),
  );

  try {
    await queue.waitUntilReady();
    await second.waitUntilReady();
    await worker.waitUntilReady();
    const completed = new Promise<void>((resolve) => worker.once('completed', () => resolve()));
    await queue.add('complete', { secret: 'must-not-appear' }, { jobId: 'complete-1' });
    await completed;
    const failed = new Promise<void>((resolve) => worker.once('failed', () => resolve()));
    await queue.add('fail', { secret: 'must-not-appear' }, { jobId: 'fail-1' });
    await failed;
    await queue.add(
      'delayed',
      { secret: 'must-not-appear' },
      { jobId: 'delayed-1', delay: 60_000 },
    );

    const before = await queue.getJobCounts('completed', 'failed', 'delayed');
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing test address');
    const base = `http://127.0.0.1:${address.port}`;
    const headers = { authorization: 'Bearer test-token' };

    const summary = await fetch(`${base}/api/v1/queues/${name}`, { headers });
    assert.equal(summary.status, 200);
    const body = await summary.json();
    assert.equal(body.success, true);
    assert.equal(body.data.counts.completed, 1);
    assert.equal(body.data.counts.failed, 1);
    assert.equal(body.data.counts.delayed, 1);
    const directory = await fetch(`${base}/api/v1/queues`, { headers });
    const listed = await directory.json();
    assert.deepEqual(
      listed.data.map((item: { name: string }) => item.name).sort(),
      [name, `${name}-second`].sort(),
    );

    const failedPage = await fetch(`${base}/api/v1/queues/${name}/jobs?state=failed&limit=1`, {
      headers,
    });
    const page = await failedPage.json();
    assert.equal(page.data.jobs[0].id, 'fail-1');
    assert.doesNotMatch(JSON.stringify(page), /must-not-appear/);

    const detail = await fetch(`${base}/api/v1/queues/${name}/jobs/fail-1`, { headers });
    assert.equal(detail.status, 200);
    const raw = await detail.text();
    assert.doesNotMatch(raw, /must-not-appear/);
    assert.equal(JSON.parse(raw).success, true);
    assert.equal(JSON.parse(raw).data.state, 'failed');
    assert.equal(JSON.parse(raw).data.failureReason, 'synthetic failure');

    const after = await queue.getJobCounts('completed', 'failed', 'delayed');
    assert.deepEqual(after, before);
    await registry.close();
    assert.equal((await queue.getJobCounts('completed')).completed, 1);
    const removed = await queue.getJob('fail-1');
    await removed?.remove();
    const gone = await fetch(`${base}/api/v1/queues/${name}/jobs/fail-1`, { headers });
    assert.equal(gone.status, 404);
    const missing = await gone.json();
    assert.equal(missing.success, false);
    assert.equal(missing.error.code, 'JOB_NOT_FOUND');
  } finally {
    server.close();
    await worker.close();
    await queue.obliterate({ force: true });
    await second.obliterate({ force: true });
    await queue.close();
    await second.close();
  }
});
