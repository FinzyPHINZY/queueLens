import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';
import { redisOptions } from '../../packages/bullmq-adapter/src/connection.js';
import { RedisAuditStore } from '../../packages/monitor/src/audit-store.js';
import { ManagementService, ManagementError } from '../../packages/monitor/src/management.js';

const redisUrl = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379';

test('real BullMQ management checks state and writes payload-free audit events', async () => {
  const name = `queueharbor-admin-${process.pid}-${Date.now()}`;
  const prefix = `queueharbor:admin:test:${process.pid}:${Date.now()}`;
  const connection = redisOptions(redisUrl);
  const queue = new Queue(name, { connection });
  const redis = new Redis(redisUrl);
  const store = new RedisAuditStore({ redisUrl, keyPrefix: prefix, requireAof: false });
  const service = new ManagementService([queue], store);
  const worker = new Worker(
    name,
    async (job) => {
      if (job.name === 'fail') throw new Error('secret-failure-text');
      return 'done';
    },
    { connection: { ...connection, maxRetriesPerRequest: null } },
  );
  try {
    await worker.waitUntilReady();
    const failed = new Promise<void>((resolve) => worker.once('failed', () => resolve()));
    const failedJob = await queue.add(
      'fail',
      { password: 'never-in-audit' },
      { removeOnFail: false },
    );
    await failed;
    await worker.close();
    const retry = await service.execute(
      'ci-admin',
      'retry',
      name,
      { jobId: failedJob.id! },
      'retry-0001',
    );
    assert.equal(retry.status, 'succeeded');
    assert.equal(await failedJob.getState(), 'waiting');
    await assert.rejects(
      service.execute('ci-admin', 'retry', name, { jobId: failedJob.id! }, 'retry-0002'),
      (cause: unknown) => cause instanceof ManagementError && cause.code === 'STATE_CHANGED',
    );
    const delayed = await queue.add('delay', {}, { delay: 60_000 });
    await service.execute('ci-admin', 'promote', name, { jobId: delayed.id! }, 'promote-0001');
    assert.equal(await delayed.getState(), 'waiting');
    await service.execute('ci-admin', 'pause', name, {}, 'pause-0001');
    assert.equal(await queue.isPaused(), true);
    await service.execute('ci-admin', 'resume', name, {}, 'resume-0001');
    assert.equal(await queue.isPaused(), false);
    const audit = await service.recentAudit(100);
    assert.ok(audit.some((entry) => entry.action === 'retry' && entry.outcome === 'succeeded'));
    assert.ok(audit.some((entry) => entry.action === 'retry' && entry.outcome === 'rejected'));
    assert.doesNotMatch(JSON.stringify(audit), /never-in-audit|secret-failure-text/);
    const keys = await redis.keys(`${prefix}:*`);
    const raw = await Promise.all(
      keys.map((key) =>
        redis.type(key).then((type) => (type === 'string' ? redis.get(key) : null)),
      ),
    );
    assert.doesNotMatch(JSON.stringify(raw), /never-in-audit|secret-failure-text/);
  } finally {
    await worker.close().catch(() => undefined);
    await queue.obliterate({ force: true });
    await queue.close();
    const keys = await redis.keys(`${prefix}:*`);
    if (keys.length) await redis.del(...keys);
    await redis.quit();
    await service.close();
  }
});
