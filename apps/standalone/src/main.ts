import { createServer } from 'node:http';
import { Queue } from 'bullmq';
import { BullMqQueueAdapter } from '../../../packages/bullmq-adapter/src/adapter.js';
import { redisOptions } from '../../../packages/bullmq-adapter/src/connection.js';
import { QueueRegistry } from '../../../packages/core/src/monitor.js';
import { createHttpHandler } from '../../../packages/server/src/http.js';

const names = (process.env.MONITOR_QUEUES ?? 'demo')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);
const token = process.env.MONITOR_TOKEN;
const port = Number(process.env.MONITOR_PORT ?? 3000);
const host = process.env.MONITOR_HOST ?? '127.0.0.1';
const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

if (!token || names.length === 0 || !Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Set MONITOR_TOKEN, at least one MONITOR_QUEUES name, and a valid MONITOR_PORT');
}

const queues = names.map(
  (name) => new BullMqQueueAdapter(new Queue(name, { connection: redisOptions(redisUrl) }), true),
);
const registry = new QueueRegistry(queues);
const server = createServer(createHttpHandler({ registry, token }));

server.listen(port, host, () => {
  console.log(`Monitor listening at http://${host}:${port}`);
});

async function shutdown() {
  server.close();
  await registry.close();
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
