import { createServer } from 'node:http';
import { resolve } from 'node:path';
import { createStandaloneMonitor } from '../../../packages/monitor/src/index.js';

const names = (process.env.MONITOR_QUEUES ?? 'demo')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);
const token = process.env.MONITOR_TOKEN;
const port = Number(process.env.MONITOR_PORT ?? 3000);
const host = process.env.MONITOR_HOST ?? '127.0.0.1';
const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const intervalMs = Number(process.env.MONITOR_POLL_INTERVAL_MS ?? 5_000);
const maxConcurrentReads = Number(process.env.MONITOR_MAX_READS ?? 4);
const queryTimeoutMs = Number(process.env.MONITOR_QUERY_TIMEOUT_MS ?? 2_000);
const payloadAccess = process.env.MONITOR_PAYLOAD_ACCESS ?? 'deny';
const showFailureDetails = process.env.MONITOR_SHOW_FAILURE_DETAILS === 'true';
const allowedPaths = (process.env.MONITOR_ALLOWED_PATHS ?? '').split(',').filter(Boolean);
const redactPaths = (process.env.MONITOR_REDACT_PATHS ?? '').split(',').filter(Boolean);

if (!token || names.length === 0 || !Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Set MONITOR_TOKEN, at least one MONITOR_QUEUES name, and a valid MONITOR_PORT');
}
if (payloadAccess !== 'deny' && payloadAccess !== 'allow') {
  throw new Error('MONITOR_PAYLOAD_ACCESS must be deny or allow');
}

const monitor = createStandaloneMonitor({
  redisUrl,
  queueNames: names,
  token,
  dashboardDir: resolve('apps/dashboard/dist'),
  polling: { intervalMs, maxConcurrentReads, queryTimeoutMs },
  redaction: { payloadAccess, allowedPaths, redactPaths, showFailureDetails },
});
const server = createServer(monitor.handler);

server.listen(port, host, () => {
  console.log(JSON.stringify({ event: 'monitor_started', host, port, queues: names.length }));
});

async function shutdown() {
  server.close();
  await monitor.close();
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
