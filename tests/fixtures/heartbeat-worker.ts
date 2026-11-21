import { Worker } from 'bullmq';
import { redisOptions } from '../../packages/bullmq-adapter/src/connection.js';
import { instrumentWorker } from '../../packages/monitor/src/worker-telemetry.js';

const name = process.argv[2];
if (!name) throw new Error('Queue name required');
const connection = redisOptions(process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379');
const worker = new Worker(name, async () => undefined, {
  connection: { ...connection, maxRetriesPerRequest: null },
});
const telemetry = instrumentWorker(worker, {
  id: 'process-worker',
  heartbeatIntervalMs: 1_000,
  heartbeatTtlMs: 2_000,
});
await worker.waitUntilReady();
await telemetry.heartbeat();
process.stdout.write('READY\n');

async function close() {
  await telemetry.close();
  await worker.close();
  process.exit(0);
}
process.once('SIGTERM', () => void close());
