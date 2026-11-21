# QueueLens

QueueLens is the working name for a self-hosted, read-only BullMQ monitor. This Stage 0 proof reads queue counts and limited job metadata. It does not yet provide a production-ready dashboard, job listing, payload access, filtering, or polling. All workspace packages are private; the public npm name is still undecided.

## Requirements

- Node.js 22, pnpm 10.20.0, and Redis. Type checking, build, lint, and socket-free unit tests have passed with Node 22 and BullMQ 5.81.5. Redis integration and broader compatibility still need verification.
- Docker Compose is optional for the demo. If Docker is unavailable, run a local `redis-server` and use the commands below.

## Local development

```bash
pnpm install --frozen-lockfile
redis-server --port 6379
# In separate terminals:
pnpm example:worker
pnpm example:seed
MONITOR_TOKEN=replace-with-a-long-random-token pnpm dev
```

Open `http://127.0.0.1:3000`, enter the token, and select **Read queues**. The page is a Stage 0 proof, not the planned React dashboard. The token stays in the page's memory and is sent as a bearer header. Avoid using a real production Redis instance for this demo.

```bash
curl -H 'Authorization: Bearer replace-with-a-long-random-token' http://127.0.0.1:3000/api/v1/queues
curl -H 'Authorization: Bearer replace-with-a-long-random-token' http://127.0.0.1:3000/api/v1/queues/demo
```

Set `REDIS_URL`, `MONITOR_QUEUES` (comma-separated), `MONITOR_HOST`, and `MONITOR_PORT` to override defaults. The server requires `MONITOR_TOKEN` and binds to `127.0.0.1` by default. `/api/v1/health` exposes only a minimal liveness response. Job responses contain metadata only; payloads, return values, failure messages, and stack traces are excluded.

## Docker demo

```bash
export MONITOR_TOKEN="$(openssl rand -hex 32)"
docker compose up --build -d
pnpm test
curl -H "Authorization: Bearer $MONITOR_TOKEN" http://127.0.0.1:3000/api/v1/queues
```

The Compose file starts Redis, a sample worker, one seed run, and the monitor at `http://127.0.0.1:3000`. The token must be set before starting Compose. The monitor listens on all interfaces _inside_ its container; Compose publishes its port on localhost only.
Redis is also published on localhost so `pnpm test` can exercise the same demo instance. The integration test uses its own temporary queue and removes it afterward.

## Checks

```bash
pnpm lint
pnpm typecheck
pnpm build
pnpm format:check
pnpm test
```

`pnpm test` includes real Redis integration tests and expects Redis at `127.0.0.1:6379`; set `TEST_REDIS_URL` to use a different test instance. Tests create and remove their own queue. `pnpm check` runs lint, type checking, build, and tests together.

## Current API contract

`GET /api/v1/queues` lists registered queues and state counts. `GET /api/v1/queues/:name` returns one snapshot. `GET /api/v1/queues/:name/jobs/:id` returns metadata without payloads. Responses use `{ data, meta, error? }`; `meta` records source and observation time. No mutations are supported. Stage 1 will add request budgets, partial-data handling, broader authorization, full redaction rules, job pages, and a proper dashboard.
