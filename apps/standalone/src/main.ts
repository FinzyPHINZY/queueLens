import { createServer } from 'node:http';
import { resolve } from 'node:path';
import { createStandaloneMonitor } from '../../../packages/monitor/src/index.js';
import { createWebhookSink, type AlertRule } from '../../../packages/monitor/src/index.js';

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
const maxReadsPerQueue = Number(process.env.MONITOR_MAX_READS_PER_QUEUE ?? 2);
const queryTimeoutMs = Number(process.env.MONITOR_QUERY_TIMEOUT_MS ?? 2_000);
const payloadAccess = process.env.MONITOR_PAYLOAD_ACCESS ?? 'deny';
const showFailureDetails = process.env.MONITOR_SHOW_FAILURE_DETAILS === 'true';
const allowedPaths = (process.env.MONITOR_ALLOWED_PATHS ?? '').split(',').filter(Boolean);
const redactPaths = (process.env.MONITOR_REDACT_PATHS ?? '').split(',').filter(Boolean);
const logLevel = process.env.MONITOR_LOG_LEVEL ?? 'info';
const levels = ['silent', 'error', 'info', 'debug'] as const;
const historyRedisUrl = process.env.MONITOR_HISTORY_REDIS_URL;
const alertRules = JSON.parse(process.env.MONITOR_ALERT_RULES_JSON ?? '[]') as AlertRule[];
if (!Array.isArray(alertRules)) throw new Error('MONITOR_ALERT_RULES_JSON must be an array');
const webhookUrl = process.env.MONITOR_ALERT_WEBHOOK_URL;
const managementEnabled = process.env.MONITOR_MANAGEMENT_ENABLED === 'true';
const adminToken = process.env.MONITOR_ADMIN_TOKEN;
const adminActorId = process.env.MONITOR_ADMIN_ACTOR_ID;
const auditRedisUrl = process.env.MONITOR_AUDIT_REDIS_URL;
const adminQueues = (process.env.MONITOR_ADMIN_QUEUES ?? '')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);

if (!token || names.length === 0 || !Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Set MONITOR_TOKEN, at least one MONITOR_QUEUES name, and a valid MONITOR_PORT');
}
if (payloadAccess !== 'deny' && payloadAccess !== 'allow') {
  throw new Error('MONITOR_PAYLOAD_ACCESS must be deny or allow');
}
if (!levels.includes(logLevel as (typeof levels)[number])) {
  throw new Error('MONITOR_LOG_LEVEL must be silent, error, info, or debug');
}
if (managementEnabled && (!adminToken || !adminActorId || !auditRedisUrl || !adminQueues.length))
  throw new Error(
    'Management requires MONITOR_ADMIN_TOKEN, MONITOR_ADMIN_ACTOR_ID, MONITOR_AUDIT_REDIS_URL, and MONITOR_ADMIN_QUEUES',
  );
function log(entry: {
  level: 'debug' | 'error' | 'info';
  event: string;
  code?: string;
  host?: string;
  port?: number;
  queues?: number;
}) {
  if (levels.indexOf(logLevel as (typeof levels)[number]) < levels.indexOf(entry.level)) return;
  console.log(JSON.stringify({ time: new Date().toISOString(), ...entry }));
}

const monitor = createStandaloneMonitor({
  redisUrl,
  queueNames: names,
  token,
  dashboardDir: resolve('apps/dashboard/dist'),
  polling: { intervalMs, maxConcurrentReads, maxReadsPerQueue, queryTimeoutMs },
  redaction: { payloadAccess, allowedPaths, redactPaths, showFailureDetails },
  log,
  history: historyRedisUrl
    ? {
        redisUrl: historyRedisUrl,
        collectionIntervalMs: Number(process.env.MONITOR_HISTORY_INTERVAL_MS ?? 60_000),
        retentionMs: Number(process.env.MONITOR_HISTORY_RETENTION_MS ?? 86_400_000),
      }
    : undefined,
  alertRules,
  notificationSinks: webhookUrl ? [createWebhookSink(webhookUrl)] : [],
  management: managementEnabled
    ? {
        enabled: true,
        auditRedisUrl: auditRedisUrl!,
        adminToken: adminToken!,
        adminActorId: adminActorId!,
        adminQueues,
      }
    : undefined,
});
const server = createServer(monitor.handler);

server.listen(port, host, () => {
  log({ level: 'info', event: 'monitor_started', host, port, queues: names.length });
});

async function shutdown() {
  server.close();
  await monitor.close();
  log({ level: 'info', event: 'monitor_stopped' });
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
