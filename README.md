# QueueLens

QueueLens is a self-hosted, read-only BullMQ monitor. Stage 2 adds optional worker heartbeats, live queue updates, bounded diagnostics, and queue comparison to the Stage 1 dashboard. **QueueLens is a working name.** The npm scope and public release are still being verified.

## Requirements

The preview is built and checked with Node.js 22, pnpm 10.20.0, BullMQ 5.81.5, and Redis 8.2. Broader versions have not been tested. The monitor never calls BullMQ mutation methods and never closes queues supplied by an embedding application.

## Run the demo

```bash
pnpm install --frozen-lockfile
pnpm build
export MONITOR_TOKEN="$(openssl rand -hex 32)"
docker compose up --build -d
```

Open <http://127.0.0.1:3000> and enter the token. Compose seeds completed and failed jobs. Redis and the monitor are published on localhost only. For a local Redis instance, run `pnpm example:worker`, `pnpm example:seed`, and `MONITOR_TOKEN=... pnpm dev` in separate terminals.

Standalone configuration: `REDIS_URL` (default `redis://127.0.0.1:6379`), `MONITOR_QUEUES` (comma-separated; default `demo`), `MONITOR_HOST` (default `127.0.0.1`), `MONITOR_PORT` (default `3000`), `MONITOR_POLL_INTERVAL_MS` (1000–60000; default 5000), `MONITOR_MAX_READS` (1–16; default 4), `MONITOR_MAX_READS_PER_QUEUE` (1–8; default 2), and `MONITOR_QUERY_TIMEOUT_MS` (100–30000; default 2000). `MONITOR_TOKEN` is required. Use TLS and an independent long random token when exposing the monitor through a trusted proxy.

Disclosure settings are `MONITOR_PAYLOAD_ACCESS=deny|allow` (default deny), comma-separated `MONITOR_ALLOWED_PATHS` and `MONITOR_REDACT_PATHS` JSON Pointers, and `MONITOR_SHOW_FAILURE_DETAILS=true` (default false). The Compose demo explicitly enables sanitized failure details for its synthetic jobs.

`MONITOR_LOG_LEVEL` accepts `silent`, `error`, `info` (default), or `debug`. Logs contain structured event names and error codes, without request URLs, tokens, payloads, or Redis exception text.

## Embed in an application

`pnpm build` prepares a typed, locally installable tarball with `pnpm --dir packages/monitor pack`. Public npm publication is pending scope verification. The package exposes `createMonitor` and `createExpressMonitor` with the same handler. Existing BullMQ queue objects remain owned by the host application.

```ts
import express from 'express';
import { Queue } from 'bullmq';
import { createExpressMonitor } from '@queuelens/monitor';

const app = express();
const queue = new Queue('email', { connection: { host: '127.0.0.1' } });
const monitor = createExpressMonitor({
  queues: [queue],
  token: process.env.MONITOR_TOKEN!,
});
app.use('/monitor', monitor.handler);
// On application shutdown: await monitor.close(); await queue.close();
```

You can also supply an async `authorize(request, target)` callback backed by your application's session and role checks, alone or alongside `token` to require both. Permissions are `queue:read` and `job:read`; queue names are passed to the callback. For callback-only session auth, the dashboard probes the same-origin API and opens without a token prompt when access is granted. `createStandaloneMonitor` owns and closes the queues it creates.

## Optional worker instrumentation

Call `instrumentWorker` beside an existing BullMQ worker to publish payload-free heartbeats to QueueLens-owned Redis keys. It uses the worker's public Redis client and never changes BullMQ job state. Close telemetry before closing the worker.

```ts
import { instrumentWorker } from '@queuelens/monitor';

const telemetry = instrumentWorker(worker, {
  id: 'email-worker-1',
  concurrency: 4,
  version: '2026.11',
  heartbeatIntervalMs: 5_000,
  heartbeatTtlMs: 15_000,
});
// On shutdown: await telemetry.close(); await worker.close();
```

Instrumented workers report heartbeat age, active job IDs, cumulative event counts, one-minute event-derived throughput, and stalled signals. A missed TTL marks a heartbeat **stale**, not a proven process death. Normal shutdown removes the record; a crashed worker remains visible until its Redis key expires. Workers without instrumentation remain **unknown**. Keep worker IDs, host labels, versions, and job IDs free of secrets.

## Read-only API

All data routes require authorization. `GET /api/v1/health` is a minimal unauthenticated liveness check. `GET /api/v1/ready` is protected and reports degraded Redis state. `GET /api/v1/queues` lists authorized queues; `GET /api/v1/queues/:name` returns counts, pause state, and oldest waiting timestamp; `GET /api/v1/queues/:name/jobs` lists jobs; `GET /api/v1/queues/:name/jobs/:id` returns one job or `JOB_NOT_FOUND`. `GET /api/v1/queues/:name/jobs/:id/flow` returns authorized parent and child links (at most 50). `GET /api/v1/workers` returns only instrumented workers. `GET /api/v1/queues/:name/diagnostics` returns bounded samples, and `GET /api/v1/events` streams authorized queue snapshots as SSE. Other methods return 405.

Job pages accept `state` (default `failed`), `limit` (1–100; default 50), `offset` (0–10000), exact `name`, `attemptsMin` (0–100), `errorCategory` for failed jobs (`timeout`, `rate_limit`, `validation`, `other`, or `unknown`), and `from`/`to` millisecond timestamps (maximum seven-day range). Filtered pages scan at most the newest 500 jobs in one state and flag partial results. `nextOffset` is a navigation hint, not a stable cursor under concurrent queue changes.

Diagnostics inspect at most the newest 100 retained completed and failed jobs in a five-minute window. Completion/failure rates, retries, wait time, and processing percentiles are **sampled**; p95 needs 20 duration samples and p99 needs 100. Removed jobs and missing collection periods can lower the displayed values. Backlog trend points are in-memory **samples** (last 60 polls), reset on monitor restart, and can contain gaps. Queue counts and pause state come from BullMQ public getters; worker throughput is **event-derived** only for instrumented workers. The dashboard reconnects SSE after interruption and polls when the stream is unavailable.

Responses use `{ success: true, data, meta }` or `{ success: false, data: null, meta, error: { code, message } }`. `meta` includes observation time, freshness, partial status, and warnings. Successful responses have no generic message; error messages are for people, while `error.code` is stable for clients. A degraded readiness check returns HTTP 503 with `success: false` and `meta.partial: true`.

Payloads, return values, failure text, and stack traces are hidden by default. Embedded callers can use `redaction: { allowedPaths: ['/user/id'], redactPaths: ['/user/password'], showFailureDetails: true }`. Paths are JSON Pointers; `*` matches one array index or object key. `payloadAccess: 'allow'` reveals full JSON unless redacted. Configure redaction carefully, because arbitrary failure text cannot be guaranteed free of secrets. Large JSON values are hidden above 64 KiB by default. Job IDs and names should also be treated as operational metadata; avoid placing secrets in them.

## Checks and troubleshooting

```bash
pnpm lint && pnpm typecheck && pnpm build && pnpm format:check
pnpm test:unit
pnpm test:integration # requires Redis at 127.0.0.1:6379 or TEST_REDIS_URL
pnpm exec playwright install chromium # once per machine
pnpm test:browser
pnpm benchmark # isolated 200-job baseline and monitored runs; requires Redis
```

When Redis is unavailable, queue snapshots can remain visible with stale warnings; job reads return `REDIS_UNAVAILABLE`. Read saturation returns `RATE_LIMITED`. Check `/api/v1/ready` with authorization and the monitor's structured startup log. Do not put Redis passwords or access tokens in issue reports.

The benchmark prints JSON with throughput, process CPU and heap, Redis command counts, and monitored API p95 latency. It creates and removes its own queue. Run it on an otherwise idle development Redis instance for a useful comparison.

### Benchmark snapshot

On one [Ubuntu 24.04 CI run](https://github.com/FinzyPHINZY/queueLens/actions/runs/44917723096) with Node 22, BullMQ 5.81.5, Redis 8.2, and 200 jobs processed for 10 ms each, the baseline completed 91.3 jobs/s with 117 ms process CPU and 6,415 Redis commands. With the monitor reading every 100 ms and polling every second, it completed 93.3 jobs/s with 146 ms CPU and 6,455 Redis commands; API p95 was 1.8 ms. End-of-run heap was 16.2 MiB and 16.0 MiB respectively. This single short run is a reproducible starting point, not a statistically meaningful worker-impact estimate.

## License

See [LICENSE](LICENSE). Local implementation checklists and product notes live under ignored `docs/` and are not committed.
