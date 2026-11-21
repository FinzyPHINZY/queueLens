import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
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

test('killed instrumented worker becomes stale and a replacement becomes online', async () => {
  const name = `monitor-worker-liveness-${process.pid}-${Date.now()}`;
  const connection = redisOptions(process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379');
  const queue = new Queue(name, { connection });
  const adapter = new BullMqQueueAdapter(queue);
  let child: ChildProcess | undefined;
  const launch = async () => {
    const processWorker = spawn(
      process.execPath,
      ['--import', 'tsx', 'tests/fixtures/heartbeat-worker.ts', name],
      {
        cwd: process.cwd(),
        env: process.env,
        stdio: ['ignore', 'pipe', 'ignore'],
      },
    );
    const ready = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('Instrumented worker did not become ready')),
        10_000,
      );
      processWorker.stdout?.on('data', (chunk: Buffer) => {
        if (chunk.toString().includes('READY')) {
          clearTimeout(timer);
          resolve();
        }
      });
      processWorker.once('exit', (code) => {
        clearTimeout(timer);
        reject(new Error(`Worker exited before ready: ${code}`));
      });
    });
    await ready;
    return processWorker;
  };
  try {
    await queue.waitUntilReady();
    child = await launch();
    const [online] = await adapter.listWorkers();
    assert.equal(online?.state, 'online');
    child.kill('SIGKILL');
    await once(child, 'exit');
    child = undefined;
    await new Promise((resolve) =>
      setTimeout(resolve, Math.max(0, (online?.lastHeartbeat ?? 0) + 2_100 - Date.now())),
    );
    const [stale] = await adapter.listWorkers();
    assert.equal(stale?.state, 'stale');
    child = await launch();
    const [restarted] = await adapter.listWorkers();
    assert.equal(restarted?.state, 'online');
    assert.ok((restarted?.lastHeartbeat ?? 0) > (stale?.lastHeartbeat ?? 0));
  } finally {
    if (child && child.exitCode === null) {
      child.kill('SIGTERM');
      await once(child, 'exit');
    }
    await queue.obliterate({ force: true });
    await queue.close();
  }
});
