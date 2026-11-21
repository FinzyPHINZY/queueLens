import { createHash, timingSafeEqual } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
  responseMeta,
  type ApiResponse,
  type MonitorErrorCode,
  type QueueRegistry,
} from '../../core/src/monitor.js';

export interface HttpConfig {
  registry: QueueRegistry;
  token: string;
}

function authorized(header: string | undefined, token: string): boolean {
  if (!header?.startsWith('Bearer ') || !token) return false;
  const supplied = createHash('sha256').update(header.slice(7)).digest();
  const expected = createHash('sha256').update(token).digest();
  return timingSafeEqual(supplied, expected);
}

function send<T>(res: ServerResponse, status: number, body: ApiResponse<T>): void {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  res.end(JSON.stringify(body));
}

function error(res: ServerResponse, status: number, code: MonitorErrorCode, message: string): void {
  send(res, status, { data: null, meta: responseMeta(), error: { code, message } });
}

function pathSegment(raw: string): string | null {
  try {
    const value = decodeURIComponent(raw);
    return value && value.length <= 128 && !value.includes('/') ? value : null;
  } catch {
    return null;
  }
}

const page = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>QueueLens — Stage 0</title></head>
<body><main><h1>QueueLens</h1><p>Stage 0 read-only queue proof.</p><label>Access token <input id="token" type="password" autocomplete="off"></label><button id="refresh">Read queues</button><pre id="result" aria-live="polite"></pre></main><script src="/stage0.js" defer></script></body></html>`;

const pageScript = `document.querySelector('#refresh').addEventListener('click', async () => {
  const result = document.querySelector('#result');
  result.textContent = 'Loading…';
  try {
    const token = document.querySelector('#token').value;
    const response = await fetch('/api/v1/queues', { headers: { authorization: 'Bearer ' + token } });
    result.textContent = JSON.stringify(await response.json(), null, 2);
  } catch {
    result.textContent = 'Unable to reach the monitor.';
  }
});`;

export function createHttpHandler(config: HttpConfig) {
  if (!config.token) throw new Error('A nonempty monitor access token is required');

  return async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const path = new URL(req.url ?? '/', 'http://localhost').pathname;
    if (req.method !== 'GET') {
      error(res, 405, 'INVALID_REQUEST', 'Only GET is supported in read-only mode');
      return;
    }
    if (path === '/') {
      res.writeHead(200, {
        'content-type': 'text/html; charset=utf-8',
        'cache-control': 'no-store',
        'content-security-policy':
          "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self'",
      });
      res.end(page);
      return;
    }
    if (path === '/stage0.js') {
      res.writeHead(200, {
        'content-type': 'text/javascript; charset=utf-8',
        'cache-control': 'no-store',
      });
      res.end(pageScript);
      return;
    }
    if (path === '/api/v1/health') {
      send(res, 200, { data: { status: 'ok' }, meta: responseMeta() });
      return;
    }
    if (!authorized(req.headers.authorization, config.token)) {
      error(res, 401, 'UNAUTHORIZED', 'Access token required');
      return;
    }

    const queueMatch = /^\/api\/v1\/queues\/([^/]+)$/.exec(path);
    const jobMatch = /^\/api\/v1\/queues\/([^/]+)\/jobs\/([^/]+)$/.exec(path);
    try {
      if (path === '/api/v1/queues') {
        const names = config.registry.names();
        const data = await Promise.all(names.map((name) => config.registry.snapshot(name)));
        send(res, 200, { data, meta: responseMeta() });
        return;
      }
      if (queueMatch?.[1]) {
        const name = pathSegment(queueMatch[1]);
        if (!name) return error(res, 400, 'INVALID_REQUEST', 'Invalid queue name');
        const snapshot = await config.registry.snapshot(name);
        if (!snapshot) return error(res, 404, 'QUEUE_NOT_FOUND', 'Queue not found');
        send(res, 200, { data: snapshot, meta: responseMeta() });
        return;
      }
      if (jobMatch?.[1] && jobMatch[2]) {
        const name = pathSegment(jobMatch[1]);
        const id = pathSegment(jobMatch[2]);
        if (!name || !id) return error(res, 400, 'INVALID_REQUEST', 'Invalid queue name or job ID');
        const queue = config.registry.get(name);
        if (!queue) return error(res, 404, 'QUEUE_NOT_FOUND', 'Queue not found');
        const job = await queue.getJobMetadata(id);
        if (!job) return error(res, 404, 'JOB_NOT_FOUND', 'Job not found');
        send(res, 200, { data: job, meta: responseMeta() });
        return;
      }
      error(res, 404, 'INVALID_REQUEST', 'Route not found');
    } catch {
      error(res, 503, 'REDIS_UNAVAILABLE', 'Queue data is unavailable');
    }
  };
}
