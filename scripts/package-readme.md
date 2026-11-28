# @queueharbor/monitor

QueueHarbor is a self-hosted dashboard and API for BullMQ queues. Inspect queue counts, jobs, failures, and worker signals without changing your job processors. Monitoring is read-only by default; queue management requires a separate, explicitly enabled admin mode.

## Install

```bash
npm install @queueharbor/monitor bullmq
```

This preview is an ESM package for Node.js 22. The tested stack is BullMQ 5.81.5 and Redis 8.2. Other versions and CommonJS `require()` have not been verified.

## Start with one queue

Create `monitor.mjs`:

```js
import { createServer } from 'node:http';
import { Queue } from 'bullmq';
import { createMonitor } from '@queueharbor/monitor';

const token = process.env.MONITOR_TOKEN;
if (!token) throw new Error('Set MONITOR_TOKEN');

const queue = new Queue('email', {
  connection: { host: '127.0.0.1', port: 6379 },
});
const monitor = createMonitor({ queues: [queue], token });
const server = createServer(monitor.handler);

server.listen(3000, '127.0.0.1');
```

Run it with a private token:

```bash
export MONITOR_TOKEN="$(openssl rand -hex 32)"
node monitor.mjs
```

Open <http://127.0.0.1:3000> and enter the token, or call the API:

```bash
curl -H "Authorization: Bearer $MONITOR_TOKEN" http://127.0.0.1:3000/api/v1/queues
```

Pass existing `Queue` instances when embedding QueueHarbor in an application. The monitor does not close queues supplied by your application: call `await monitor.close()` and close your queues during shutdown. Use `createExpressMonitor` to mount the same handler with `app.use('/monitor', monitor.handler)`. For a monitor that owns its queue connections, use `createStandaloneMonitor({ redisUrl, queueNames, token })` and serve its handler with Node's HTTP server.

## Defaults and optional features

- Every data route requires a bearer token or an application `authorize` callback. The minimal `/api/v1/health` liveness route is public.
- Job payloads, return values, failure text, and stack traces are hidden by default. Configure disclosure deliberately; free-form error text can contain secrets.
- Queue and job reads are bounded. The dashboard shows observation time, partial results, and stale data when Redis is unavailable.
- Worker heartbeats, aggregate history, alerts, and audited management are opt-in. A protected Prometheus metrics endpoint is available. Uninstrumented workers have unknown liveness; the default monitor cannot retry, remove, or pause jobs.

See the [full documentation](https://github.com/FinzyPHINZY/queueharbor#readme) for configuration, authorization, redaction, Docker deployment, and management safeguards. Report problems in [GitHub Issues](https://github.com/FinzyPHINZY/queueharbor/issues). Licensed under [MIT](https://github.com/FinzyPHINZY/queueharbor/blob/main/LICENSE).
