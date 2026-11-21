import assert from 'node:assert/strict';
import { test } from 'node:test';
import { FlowProducer, Queue, Worker } from 'bullmq';
import { BullMqQueueAdapter } from '../../packages/bullmq-adapter/src/adapter.js';
import { redisOptions } from '../../packages/bullmq-adapter/src/connection.js';
import { sampleQueueDiagnostics } from '../../packages/core/src/diagnostics.js';
import { instrumentWorker } from '../../packages/monitor/src/worker-telemetry.js';

test('real BullMQ worker heartbeat and sampled diagnostics stay payload-free', async () => {
  const name = `monitor-stage2-${process.pid}-${Date.now()}`;
  const connection = redisOptions(process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379');
  const queue = new Queue(name, { connection });
  const flowProducer = new FlowProducer({ connection });
  const worker = new Worker(name, async () => 'done', {
    connection: { ...connection, maxRetriesPerRequest: null },
    concurrency: 2,
  });
  const adapter = new BullMqQueueAdapter(queue);
  const telemetry = instrumentWorker(worker, {
    id: 'integration-worker',
    concurrency: 2,
    version: 'test',
    heartbeatIntervalMs: 1_000,
    heartbeatTtlMs: 2_000,
  });
  try {
    await worker.waitUntilReady();
    await telemetry.heartbeat();
    const [initial] = await adapter.listWorkers();
    assert.equal(initial?.state, 'online');
    assert.equal(initial?.concurrency, 2);
    const completed = new Promise<void>((resolve) => worker.once('completed', () => resolve()));
    await queue.add('safe', { secret: 'must-not-appear' });
    await completed;
    await telemetry.heartbeat();
    const [observed] = await adapter.listWorkers();
    assert.equal(observed?.completed, 1);
    assert.equal(observed?.throughput.completed, 1);
    assert.doesNotMatch(JSON.stringify(observed), /must-not-appear/);
    const diagnostics = await sampleQueueDiagnostics(adapter, (operation) => operation(), []);
    assert.equal(diagnostics.source, 'sampled');
    assert.equal(diagnostics.completed, 1);
    assert.doesNotMatch(JSON.stringify(diagnostics), /must-not-appear/);
    const flow = await flowProducer.add({
      name: 'parent',
      queueName: name,
      data: {},
      children: [{ name: 'child', queueName: name, data: {} }],
    });
    const parentId = flow.job.id;
    const childId = flow.children?.[0]?.job.id;
    assert.ok(parentId && childId);
    const parentLinks = await adapter.getFlow(parentId);
    assert.ok(parentLinks?.childKeys.some((key) => key.endsWith(`:${childId}`)));
    const childLinks = await adapter.getFlow(childId);
    assert.equal(childLinks?.parent?.id, parentId);
    await queue.upsertJobScheduler(
      'schedule-1',
      { every: 60_000 },
      { name: 'scheduled', data: { secret: 'must-not-appear' } },
    );
    const schedulers = await adapter.listSchedulers();
    assert.ok(schedulers.some((item) => item.key === 'schedule-1'));
    assert.doesNotMatch(JSON.stringify(schedulers), /must-not-appear/);
  } finally {
    await telemetry.close();
    await worker.close();
    await flowProducer.close();
    await queue.obliterate({ force: true });
    await queue.close();
  }
});
