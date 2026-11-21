import { createHash, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
  observedStates,
  responseMeta,
  type ApiResponse,
  type MonitorErrorCode,
  type ObservedState,
  type QueueRegistry,
  type QueueSnapshot,
} from '../../core/src/monitor.js';
import { SnapshotPoller, type PollingOptions } from '../../core/src/poller.js';
import { ReadBudgetExceededError, ReadTimeoutError } from '../../core/src/read-budget.js';
import {
  safeJobDetail,
  safeJobSummary,
  validateDisclosurePolicy,
  type DisclosurePolicy,
} from '../../core/src/redaction.js';

export interface AuthTarget {
  queueName?: string;
  permission: 'queue:read' | 'job:read';
}

export type Authorize = (
  request: IncomingMessage,
  target: AuthTarget,
) => boolean | Promise<boolean>;

export interface HttpConfig {
  registry: QueueRegistry;
  token?: string;
  authorize?: Authorize;
  polling?: PollingOptions;
  poller?: SnapshotPoller;
  redaction?: DisclosurePolicy;
  dashboardDir?: string;
  log?: (entry: {
    level: 'debug' | 'error';
    event: 'api_read' | 'read_failed';
    code?: MonitorErrorCode;
  }) => void;
}

function tokenMatches(header: string | undefined, token: string): boolean {
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

function segment(raw: string): string | null {
  try {
    const value = decodeURIComponent(raw);
    return value && value.length <= 128 && !value.includes('/') ? value : null;
  } catch {
    return null;
  }
}

function positiveInteger(value: string | null, fallback: number, maximum: number): number | null {
  if (value === null) return fallback;
  if (!/^\d+$/.test(value)) return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 && number <= maximum ? number : null;
}

function timestamp(value: string | null): number | null | undefined {
  if (value === null) return undefined;
  if (!/^\d+$/.test(value)) return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number > 0 ? number : null;
}

function failureStatus(cause: unknown): {
  status: number;
  code: MonitorErrorCode;
  message: string;
} {
  if (cause instanceof ReadBudgetExceededError) {
    return { status: 429, code: 'RATE_LIMITED', message: 'Monitor read budget is busy' };
  }
  if (cause instanceof ReadTimeoutError) {
    return { status: 503, code: 'REDIS_UNAVAILABLE', message: 'Queue read timed out' };
  }
  return { status: 503, code: 'REDIS_UNAVAILABLE', message: 'Queue data is unavailable' };
}

const fallbackPage = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>QueueLens</title></head><body><h1>QueueLens</h1><p>Build the dashboard with <code>pnpm dashboard:build</code> to enable the interface.</p></body></html>`;

async function serveDashboard(
  path: string,
  res: ServerResponse,
  dashboardDir: string | undefined,
): Promise<boolean> {
  if (path !== '/' && !/^\/assets\/[A-Za-z0-9._-]+$/.test(path)) return false;
  const file = path === '/' ? 'index.html' : path.slice(1);
  const mime = file.endsWith('.js')
    ? 'text/javascript; charset=utf-8'
    : file.endsWith('.css')
      ? 'text/css; charset=utf-8'
      : 'text/html; charset=utf-8';
  try {
    if (!dashboardDir) throw new Error('No dashboard build');
    const contents = await readFile(join(dashboardDir, file));
    res.writeHead(200, {
      'content-type': mime,
      'cache-control': path === '/' ? 'no-store' : 'public, max-age=31536000, immutable',
      'x-content-type-options': 'nosniff',
      'content-security-policy':
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:",
    });
    res.end(contents);
  } catch {
    if (path !== '/') return false;
    res.writeHead(200, { 'content-type': mime, 'cache-control': 'no-store' });
    res.end(fallbackPage);
  }
  return true;
}

export function createHttpHandler(config: HttpConfig) {
  if (!config.token && !config.authorize)
    throw new Error('A monitor authorization strategy is required');
  validateDisclosurePolicy(config.redaction);
  const poller = config.poller ?? new SnapshotPoller(config.registry, config.polling);

  async function permitted(req: IncomingMessage, target: AuthTarget): Promise<boolean> {
    if (config.token && !tokenMatches(req.headers.authorization, config.token)) return false;
    return config.authorize ? Boolean(await config.authorize(req, target)) : true;
  }

  return async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    if (
      (req.url?.length ?? 0) > 2_048 ||
      req.headers['transfer-encoding'] ||
      Number(req.headers['content-length'] ?? 0) > 0
    ) {
      return error(res, 413, 'INVALID_REQUEST', 'Request is too large');
    }
    let url: URL;
    try {
      url = new URL(req.url ?? '/', 'http://localhost');
    } catch {
      return error(res, 400, 'INVALID_REQUEST', 'Invalid URL');
    }
    const path = url.pathname;
    config.log?.({ level: 'debug', event: 'api_read' });
    if (req.method !== 'GET')
      return error(res, 405, 'INVALID_REQUEST', 'Read-only mode permits GET only');
    const originalUrl = (req as IncomingMessage & { originalUrl?: string }).originalUrl;
    if (path === '/' && originalUrl && !originalUrl.split('?')[0]?.endsWith('/')) {
      res.writeHead(308, {
        location: `${originalUrl.split('?')[0]}/`,
        'cache-control': 'no-store',
      });
      res.end();
      return;
    }
    if (await serveDashboard(path, res, config.dashboardDir)) return;
    if (path === '/api/v1/health') {
      return send(res, 200, { data: { status: 'ok' }, meta: responseMeta() });
    }
    if (config.token && !tokenMatches(req.headers.authorization, config.token)) {
      return error(res, 401, 'UNAUTHORIZED', 'Access token required');
    }

    try {
      if (path === '/api/v1/ready') {
        if (!(await permitted(req, { permission: 'queue:read' }))) {
          return error(res, 403, 'FORBIDDEN', 'Access denied');
        }
        const result = await poller.getAll();
        return send(res, result.meta.partial ? 503 : 200, {
          data: { status: result.meta.partial ? 'degraded' : 'ready' },
          meta: { ...result.meta, warnings: [] },
        });
      }
      if (path === '/api/v1/workers') {
        if (!(await permitted(req, { permission: 'queue:read' }))) {
          return error(res, 403, 'FORBIDDEN', 'Access denied');
        }
        return send(res, 200, {
          data: {
            liveness: 'unknown',
            message: 'Per-worker liveness requires opt-in instrumentation.',
          },
          meta: responseMeta(),
        });
      }
      if (path === '/api/v1/queues') {
        const names = config.registry.names();
        const allowed: string[] = [];
        for (const name of names) {
          if (await permitted(req, { queueName: name, permission: 'queue:read' }))
            allowed.push(name);
        }
        const results = await Promise.allSettled(allowed.map((name) => poller.get(name)));
        const snapshots: QueueSnapshot[] = [];
        const warnings: string[] = [];
        let partial = false;
        let observedAt = 0;
        for (let index = 0; index < results.length; index++) {
          const result = results[index];
          if (result?.status === 'fulfilled') {
            snapshots.push(result.value.data);
            partial ||= result.value.meta.partial;
            warnings.push(...result.value.meta.warnings);
            observedAt = Math.max(observedAt, Date.parse(result.value.meta.observedAt));
          } else {
            partial = true;
            warnings.push(`${allowed[index]}: unavailable`);
          }
        }
        if (allowed.length > 0 && snapshots.length === 0) {
          return error(res, 503, 'REDIS_UNAVAILABLE', 'Queue data is unavailable');
        }
        const meta = responseMeta(new Date(observedAt || Date.now()));
        meta.partial = partial;
        meta.warnings = warnings;
        meta.freshnessMs = observedAt ? Date.now() - observedAt : 0;
        return send(res, 200, { data: snapshots, meta });
      }

      const jobsMatch = /^\/api\/v1\/queues\/([^/]+)\/jobs$/.exec(path);
      const jobMatch = /^\/api\/v1\/queues\/([^/]+)\/jobs\/([^/]+)$/.exec(path);
      const queueMatch = /^\/api\/v1\/queues\/([^/]+)$/.exec(path);
      const rawName = jobsMatch?.[1] ?? jobMatch?.[1] ?? queueMatch?.[1];
      if (!rawName) return error(res, 404, 'INVALID_REQUEST', 'Route not found');
      const name = segment(rawName);
      if (!name) return error(res, 400, 'INVALID_REQUEST', 'Invalid queue name');
      const queue = config.registry.get(name);
      if (!queue) return error(res, 404, 'QUEUE_NOT_FOUND', 'Queue not found');
      const permission = jobsMatch || jobMatch ? 'job:read' : 'queue:read';
      if (!(await permitted(req, { queueName: name, permission }))) {
        return error(res, 403, 'FORBIDDEN', 'Access denied');
      }

      if (queueMatch) {
        const snapshot = await poller.get(name);
        return send(res, 200, snapshot);
      }
      if (jobMatch?.[2]) {
        const id = segment(jobMatch[2]);
        if (!id) return error(res, 400, 'INVALID_REQUEST', 'Invalid job ID');
        const job = await poller.budget.run(() => queue.getJob(id));
        if (!job) return error(res, 404, 'JOB_NOT_FOUND', 'Job not found');
        return send(res, 200, { data: safeJobDetail(job, config.redaction), meta: responseMeta() });
      }
      if (jobsMatch) {
        const state = url.searchParams.get('state') ?? 'failed';
        const limit = positiveInteger(url.searchParams.get('limit'), 50, 100);
        const offset = positiveInteger(url.searchParams.get('offset'), 0, 10_000);
        const from = timestamp(url.searchParams.get('from'));
        const to = timestamp(url.searchParams.get('to'));
        const filterName = url.searchParams.get('name');
        if (
          !observedStates.includes(state as ObservedState) ||
          !limit ||
          offset === null ||
          from === null ||
          to === null ||
          (from && to && (from > to || to - from > 7 * 24 * 60 * 60 * 1_000)) ||
          (filterName && filterName.length > 128)
        ) {
          return error(res, 400, 'INVALID_FILTER', 'Invalid job filter');
        }
        const filtered = Boolean(filterName || from || to);
        if (filtered && offset > 500)
          return error(res, 400, 'INVALID_FILTER', 'Filtered offset exceeds 500');
        const scanLimit = filtered ? 500 : limit;
        const raw = await poller.budget.run(() =>
          queue.listJobs({
            state: state as ObservedState,
            offset: filtered ? 0 : offset,
            limit: scanLimit,
          }),
        );
        const matches = filtered
          ? raw.filter(
              (job) =>
                (!filterName || job.name === filterName) &&
                (!from || job.timestamp >= from) &&
                (!to || job.timestamp <= to),
            )
          : raw;
        const page = filtered ? matches.slice(offset, offset + limit) : matches;
        const meta = responseMeta();
        meta.partial = filtered && raw.length === scanLimit;
        if (meta.partial)
          meta.warnings.push(
            'Name/time filtering inspected only the 500 newest jobs in this state',
          );
        return send(res, 200, {
          data: {
            jobs: page.map(safeJobSummary),
            nextOffset: page.length === limit ? offset + limit : null,
          },
          meta,
        });
      }
    } catch (cause) {
      const failure = failureStatus(cause);
      config.log?.({ level: 'error', event: 'read_failed', code: failure.code });
      return error(res, failure.status, failure.code, failure.message);
    }
  };
}
