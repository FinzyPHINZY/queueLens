# QueueHarbor

[![npm @queueharbor/monitor](https://img.shields.io/npm/v/%40queueharbor%2Fmonitor/latest.svg?label=npm%20%40queueharbor%2Fmonitor)](https://www.npmjs.com/package/@queueharbor/monitor)
[![CI](https://github.com/FinzyPHINZY/queueharbor/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/FinzyPHINZY/queueharbor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE)

QueueHarbor is a self-hosted dashboard and API for BullMQ. It reads queue state, jobs, failures, and worker signals without requiring changes to existing job processors. Monitoring is read-only by default. Queue actions are available only through a separately enabled, audited admin mode.

## Features

- Inspect multiple queues, job details, retries, flows, and scheduler metadata.
- See bounded diagnostics, observation timestamps, partial results, and stale data during Redis outages.
- Add optional worker heartbeats, aggregate history, alerts, and Prometheus metrics.
- Embed the monitor in an application or run it as a standalone service. No hosted account is required.

## Compatibility

| Component     | Supported or tested                                       |
| ------------- | --------------------------------------------------------- |
| Node.js       | `>=22 <23`; tested on Node 22                             |
| BullMQ        | Peer dependency `^5.81.5`; tested with 5.81.5             |
| Redis         | Tested with 8.2; no wider range claimed                   |
| Module format | ESM with TypeScript declarations; no CommonJS entry point |

The npm package is [`@queueharbor/monitor`](https://www.npmjs.com/package/@queueharbor/monitor). This is a preview release; check the compatibility set before deploying it with other versions.

## Try the dashboard

The Docker Compose demo starts Redis, a worker, a seed job, and QueueHarbor. It binds Redis and the dashboard to localhost.

```bash
export MONITOR_TOKEN="$(openssl rand -hex 32)"
docker compose up --build -d
```

Open <http://127.0.0.1:3000> and enter the token. The seed creates completed and failed jobs. The History view needs one sampling interval before it shows a point. Stop the demo with `docker compose down`.

To use a local Redis instance without Compose, run `pnpm example:worker`, `pnpm example:seed`, and `MONITOR_TOKEN=... pnpm dev` in separate terminals after `pnpm install --frozen-lockfile`.

## Add it to an application

Install the package alongside your application's BullMQ dependency:

```bash
npm install @queueharbor/monitor bullmq
```

Pass an existing `Queue` to the Express adapter. In this example, `app` and `emailQueue` are your application's Express app and BullMQ queue:

```ts
import { createExpressMonitor } from '@queueharbor/monitor';

const token = process.env.MONITOR_TOKEN;
if (!token) throw new Error('Set MONITOR_TOKEN');

const monitor = createExpressMonitor({
  queues: [emailQueue],
  token,
});
app.use('/monitor', monitor.handler);

// During application shutdown:
await monitor.close();
await emailQueue.close();
```

The dashboard is mounted at `/monitor`; its API is under `/monitor/api/v1/`. QueueHarbor never closes queues supplied by the application. `createMonitor` exposes the same HTTP handler for Node's `createServer`, while `createStandaloneMonitor({ redisUrl, queueNames, token })` creates and owns its queue connections. The [package quick start](scripts/package-readme.md) has a complete Node.js example.

For session-based access, provide `authorize(request, target)` instead of a bearer token. The callback receives `queue:read` or `job:read` and the relevant queue name. If both `token` and `authorize` are supplied, both checks must pass.

## Configuration and security

The standalone app (`pnpm dev` or the built image) uses these environment variables:

| Variable                                         | Default                  | Purpose                                                               |
| ------------------------------------------------ | ------------------------ | --------------------------------------------------------------------- |
| `MONITOR_TOKEN`                                  | Required                 | Bearer token for dashboard data and read APIs.                        |
| `REDIS_URL`                                      | `redis://127.0.0.1:6379` | BullMQ Redis connection.                                              |
| `MONITOR_QUEUES`                                 | `demo`                   | Comma-separated queue names.                                          |
| `MONITOR_HOST` / `MONITOR_PORT`                  | `127.0.0.1` / `3000`     | HTTP bind address and port.                                           |
| `MONITOR_LOG_LEVEL`                              | `info`                   | `silent`, `error`, `info`, or `debug`.                                |
| `MONITOR_PAYLOAD_ACCESS`                         | `deny`                   | Set to `allow` only when payload disclosure is intended.              |
| `MONITOR_ALLOWED_PATHS` / `MONITOR_REDACT_PATHS` | Empty                    | Comma-separated JSON Pointers for selective disclosure and redaction. |
| `MONITOR_SHOW_FAILURE_DETAILS`                   | `false`                  | Show sanitized failure text and stack traces.                         |
| `MONITOR_HISTORY_REDIS_URL`                      | Disabled                 | Enable aggregate history and incident storage.                        |

Polling defaults to five seconds. `MONITOR_POLL_INTERVAL_MS` accepts 1000–60000, `MONITOR_MAX_READS` accepts 1–16, `MONITOR_MAX_READS_PER_QUEUE` accepts 1–8, and `MONITOR_QUERY_TIMEOUT_MS` accepts 100–30000. See [`apps/standalone/src/main.ts`](apps/standalone/src/main.ts) for the full environment mapping.

All data routes require authorization. Only the minimal `/api/v1/health` liveness check is public. Payloads, return values, failure text, and stacks are hidden by default; configure `redaction` or the standalone disclosure variables deliberately. JSON values above 64 KiB are hidden by default. Free-form failure text, job names, and job IDs may still contain secrets. Use TLS and a distinct, long token when exposing the monitor through a trusted proxy, and keep Redis credentials out of issue reports.

### Optional worker telemetry

Call `instrumentWorker` beside an existing BullMQ worker to publish payload-free heartbeats. It uses the worker's public Redis client and does not alter job state:

```ts
import { instrumentWorker } from '@queueharbor/monitor';

const telemetry = instrumentWorker(worker, {
  id: 'email-worker-1',
  concurrency: 4,
});
// On shutdown: await telemetry.close(); await worker.close();
```

Instrumented workers report heartbeat age and event-derived activity. A missed heartbeat is **stale**, not proof of process death; uninstrumented worker liveness remains **unknown**. Do not put secrets in worker IDs or host labels.

### Optional history and alerts

Set `MONITOR_HISTORY_REDIS_URL` to collect payload-free aggregate samples in monitor-owned Redis keys. Sampling defaults to 60 seconds and retention to 24 hours; missed periods appear as gaps. History and incident state need Redis persistence to survive Redis loss.

Alerts are configured on the server, not in the dashboard. A backlog example:

```bash
export MONITOR_HISTORY_REDIS_URL=redis://127.0.0.1:6379
export MONITOR_ALERT_WEBHOOK_URL=https://alerts.example.com/queueharbor
export MONITOR_ALERT_RULES_JSON='[{"id":"email-backlog","queue":"email","metric":"backlog","threshold":10,"sustainMs":120000,"cooldownMs":300000,"channel":"webhook","enabled":true}]'
```

Rules support `backlog`, `failed_count`, sampled `failure_rate`, `waiting_age`, and instrumented-worker `heartbeat_gap`. A sustained window needs consecutive samples; gaps reset it. Webhook delivery is at least once and includes an `x-queuelens-event-id` for deduplication. Embedded applications can instead provide `createEmailSink` with their own transport. Alert configuration changes require a monitor restart.

### Explicit management mode

Standalone queue mutations are disabled unless `MONITOR_MANAGEMENT_ENABLED=true`; embedded mode requires `management: { enabled: true, ... }`. Standalone management also requires `MONITOR_ADMIN_TOKEN` (distinct from the read token and at least 32 characters), `MONITOR_ADMIN_ACTOR_ID`, `MONITOR_ADMIN_QUEUES`, and `MONITOR_AUDIT_REDIS_URL`. The audit Redis must have AOF enabled; QueueHarbor waits for an AOF fsync before acting and fails closed if audit storage is unavailable. Keep its data on a persistent volume and back it up.

Admin actions have separate authorization, per-queue scope, idempotency keys, and audit events. Bulk actions require a preview and recheck each job; `cancel-request` is advisory and needs processor cooperation. Embedded, cookie-authenticated management requires host-provided actor authorization and CSRF verification. Review the [management implementation](packages/monitor/src/management.ts) and [integration tests](tests/integration/stage4.test.ts) before enabling writes in production.

## HTTP API and data limits

| Route                                                                  | Purpose                                   |
| ---------------------------------------------------------------------- | ----------------------------------------- |
| `GET /api/v1/health`, `/api/v1/ready`                                  | Liveness and protected Redis readiness.   |
| `GET /api/v1/queues`, `/api/v1/queues/:name`                           | Queue list, counts, and status.           |
| `GET /api/v1/queues/:name/jobs`, `/api/v1/queues/:name/jobs/:id`       | Bounded job pages and exact lookup.       |
| `GET /api/v1/workers`, `/api/v1/events`                                | Instrumented workers and SSE snapshots.   |
| `GET /api/v1/queues/:name/diagnostics`, `/api/v1/queues/:name/history` | Sampled diagnostics and optional history. |
| `GET /api/v1/alerts/*`, `/api/v1/metrics`                              | Alerts and protected Prometheus metrics.  |

Successful JSON responses use `{ success: true, data, meta }`; errors use `{ success: false, data: null, meta, error: { code, message } }`. `meta` includes observation time, freshness, partial status, and warnings. Degraded readiness returns HTTP 503.

Job pages default to 50 failed jobs and cap at 100 per page. Offset caps at 10,000; name and time filters inspect at most the newest 500 jobs in one state, and time ranges cap at seven days. Offset pages can shift under queue activity. Diagnostics sample at most 100 retained completed and failed jobs over five minutes. Removed jobs and gaps can lower rates; these samples are not exact totals. Queue counts come from BullMQ public getters.

## Development

```bash
pnpm install --frozen-lockfile
pnpm format:check
pnpm check          # lint, typecheck, build, unit and Redis integration tests
pnpm test:package   # external tarball install, ESM import, and TypeScript compile
pnpm exec playwright install chromium # once per machine
pnpm test:browser
pnpm benchmark      # isolated baseline and monitored runs; requires Redis
```

`pnpm check` and the browser and benchmark tests require Redis at `127.0.0.1:6379` or `TEST_REDIS_URL`. `pnpm test:package` downloads dependencies into a temporary external project. CI also builds and smokes the Docker image.

Core contracts and polling live in `packages/core/`; `packages/bullmq-adapter/` reads BullMQ through public APIs; `packages/monitor/` assembles the package; and `packages/server/` serves HTTP. The standalone app, dashboard, and demo worker live under `apps/`. Tests are in `tests/unit/`, `tests/integration/`, and `tests/browser/`.

## Operations

For monitor upgrades, retain environment settings and a Redis backup, especially the AOF-backed audit store. Stop the monitor, deploy the new version, then check authorized `/api/v1/ready`, queue counts, and the dashboard. Workers can keep processing during a monitor-only replacement. Roll back to the saved version and settings if those checks fail; do not delete audit or history keys.

The QueueHarbor name does not change existing `queuelens:` Redis keys, `queuelens_` metrics, or the `x-queuelens-event-id` webhook header. These identifiers remain stable for existing integrations.

## Benchmark snapshot

In one [Ubuntu 24.04 CI run](https://github.com/FinzyPHINZY/queueharbor/actions/runs/44917723096), Node 22, BullMQ 5.81.5, and Redis 8.2 processed 200 jobs with 10 ms of work each:

| Measure          | Baseline | Monitor reading every 100 ms, polling every second |
| ---------------- | -------: | -------------------------------------------------: |
| Completed jobs/s |     91.3 |                                               93.3 |
| Process CPU      |   117 ms |                                             146 ms |
| Redis commands   |    6,415 |                                              6,455 |
| API p95 latency  |        — |                                             1.8 ms |

This short single run is a reproducible baseline, not a statistically reliable worker-impact estimate. Run `pnpm benchmark` on an otherwise idle development Redis instance for a local comparison.

## Contributing

Open an issue for substantial changes before implementing them. For pull requests, describe observable behavior and security implications, list the checks you ran, and include screenshots for dashboard changes. Keep BullMQ calls behind the adapter and preserve the read-only default.

## Support and license

Report bugs and request features through [GitHub Issues](https://github.com/FinzyPHINZY/queueharbor/issues). Include the QueueHarbor, Node, BullMQ, and Redis versions and a minimal reproduction; omit tokens, Redis credentials, and customer payloads. QueueHarbor is [MIT licensed](LICENSE).
