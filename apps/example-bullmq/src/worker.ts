import { Worker } from 'bullmq';
import { redisOptions } from '../../../packages/bullmq-adapter/src/connection.js';
import { instrumentWorker } from '../../../packages/monitor/src/index.js';

const worker = new Worker(
  'demo',
  async (job) => {
    if (job.name === 'fail') throw new Error('Synthetic demo failure');
    if (job.name === 'slow') await new Promise((resolve) => setTimeout(resolve, 30_000));
    return { ok: true };
  },
  {
    connection: {
      ...redisOptions(process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'),
      maxRetriesPerRequest: null,
    },
    concurrency: 1,
  },
);

worker.on('completed', (job) => console.log(`Completed ${job.id}`));
worker.on('failed', (job) => console.log(`Failed ${job?.id}`));
worker.on('error', (cause) => console.error('Demo worker error:', cause.message));
const telemetry = instrumentWorker(worker, { concurrency: 1, version: 'demo' });

async function shutdown() {
  await telemetry.close();
  await worker.close();
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
