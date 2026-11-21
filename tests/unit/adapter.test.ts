import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Queue } from 'bullmq';
import { BullMqQueueAdapter } from '../../packages/bullmq-adapter/src/adapter.js';

test('adapter uses only public getters and leaves caller-owned queues open', async () => {
  let closed = false;
  const allowed = {
    name: 'demo',
    async getJobCounts() {
      return { waiting: 2, active: 1 };
    },
    async getJob() {
      return {
        id: 'job-1',
        name: 'sample',
        timestamp: 100,
        attemptsMade: 0,
        data: { password: 'never-expose' },
      };
    },
    async getJobState() {
      return 'waiting';
    },
    async close() {
      closed = true;
    },
  };
  const queue = new Proxy(allowed, {
    get(target, property) {
      if (!(property in target)) throw new Error(`Unexpected BullMQ method: ${String(property)}`);
      return Reflect.get(target, property);
    },
  }) as unknown as Queue;

  const adapter = new BullMqQueueAdapter(queue);
  const counts = await adapter.getCounts();
  assert.equal(counts.waiting, 2);
  assert.equal(counts.active, 1);
  assert.equal(counts.failed, 0);

  const job = await adapter.getJobMetadata('job-1');
  assert.equal(job?.id, 'job-1');
  assert.doesNotMatch(JSON.stringify(job), /never-expose/);

  await adapter.close();
  assert.equal(closed, false);
});
