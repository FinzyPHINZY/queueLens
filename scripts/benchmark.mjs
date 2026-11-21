import { createServer } from 'node:http';
import { performance } from 'node:perf_hooks';
import process from 'node:process';
import { clearInterval, setInterval, setTimeout } from 'node:timers';
import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';
import { redisOptions } from '../dist/packages/bullmq-adapter/src/connection.js';
import { createMonitor } from '../dist/packages/monitor/src/index.js';

const redisUrl = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379';
const token = 'local-benchmark-token';
const name = `queuelens-bench-${process.pid}`;
const connection = redisOptions(redisUrl);
const queue = new Queue(name, { connection });
const redis = new Redis(redisUrl);
const worker = new Worker(name, async () => new Promise((resolve) => setTimeout(resolve, 10)), {
  connection: { ...connection, maxRetriesPerRequest: null },
});

async function commandCount() {
  const info = await redis.info('commandstats');
  return [...info.matchAll(/calls=(\d+)/g)].reduce((total, match) => total + Number(match[1]), 0);
}

async function run(label, monitored) {
  const monitor = monitored
    ? createMonitor({ queues: [queue], token, polling: { intervalMs: 1_000 } })
    : null;
  const server = monitor ? createServer(monitor.handler) : null;
  if (server) await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server?.address();
  const endpoint =
    address && typeof address !== 'string'
      ? `http://127.0.0.1:${address.port}/api/v1/queues`
      : null;
  const countBefore = await commandCount();
  const cpuBefore = process.cpuUsage();
  const start = performance.now();
  const latencies = [];
  let completed = 0;
  const done = new Promise((resolve) => {
    const onCompleted = () => {
      completed++;
      if (completed === 200) {
        worker.off('completed', onCompleted);
        resolve();
      }
    };
    worker.on('completed', onCompleted);
  });
  const polling = endpoint
    ? setInterval(() => {
        const started = performance.now();
        void globalThis
          .fetch(endpoint, { headers: { Authorization: `Bearer ${token}` } })
          .then((response) => {
            if (response.ok) latencies.push(performance.now() - started);
          })
          .catch(() => undefined);
      }, 100)
    : null;
  await queue.addBulk(
    Array.from({ length: 200 }, (_, index) => ({
      name: 'benchmark',
      data: { index },
      opts: { removeOnComplete: true },
    })),
  );
  await done;
  if (polling) clearInterval(polling);
  const elapsedMs = performance.now() - start;
  const cpu = process.cpuUsage(cpuBefore);
  const commandDelta = (await commandCount()) - countBefore;
  if (server) await new Promise((resolve) => server.close(resolve));
  await monitor?.close();
  latencies.sort((a, b) => a - b);
  return {
    label,
    jobs: completed,
    elapsedMs: Math.round(elapsedMs),
    throughputJobsPerSecond: Number(((completed / elapsedMs) * 1_000).toFixed(1)),
    cpuMs: Math.round((cpu.user + cpu.system) / 1_000),
    heapMb: Number((process.memoryUsage().heapUsed / 1_048_576).toFixed(1)),
    redisCommands: commandDelta,
    apiP95Ms: latencies.length
      ? Number(latencies[Math.ceil(latencies.length * 0.95) - 1].toFixed(1))
      : null,
  };
}

try {
  await queue.waitUntilReady();
  await worker.waitUntilReady();
  globalThis.console.log(JSON.stringify(await run('baseline', false)));
  globalThis.console.log(JSON.stringify(await run('monitor', true)));
} finally {
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  await redis.quit();
}
