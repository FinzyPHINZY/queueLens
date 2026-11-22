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
import { errorCategory, sampleQueueDiagnostics } from '../../core/src/diagnostics.js';
import { ReadBudgetExceededError, ReadTimeoutError } from '../../core/src/read-budget.js';
import type { HistoryReader } from '../../core/src/history.js';
import type { AlertRule, AlertStorage } from '../../core/src/alerts.js';
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
  history?: HistoryReader;
  alertStore?: AlertStorage;
  alertRules?: AlertRule[];
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

function error(
  res: ServerResponse,
  status: number,
  code: MonitorErrorCode,
  message: string,
  meta = responseMeta(),
): void {
  send(res, status, { success: false, data: null, meta, error: { code, message } });
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

  async function allowedQueues(req: IncomingMessage): Promise<string[]> {
    const allowed: string[] = [];
    for (const name of config.registry.names()) {
      if (await permitted(req, { queueName: name, permission: 'queue:read' })) allowed.push(name);
    }
    return allowed;
  }

  async function streamSnapshots(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const names = await allowedQueues(req);
    if (config.registry.names().length > 0 && names.length === 0) {
      error(res, 403, 'FORBIDDEN', 'Access denied');
      return;
    }
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-store',
      connection: 'keep-alive',
      'x-content-type-options': 'nosniff',
    });
    res.write('retry: 3000\n\n');
    let closed = false;
    let busy = false;
    const close = () => {
      closed = true;
      clearInterval(timer);
    };
    const emit = async () => {
      if (closed || busy) return;
      busy = true;
      try {
        const current = await allowedQueues(req);
        if (
          current.length !== names.length ||
          current.some((name, index) => name !== names[index])
        ) {
          res.end();
          close();
          return;
        }
        const results = await Promise.allSettled(names.map((name) => poller.get(name)));
        const data = results.flatMap((result) =>
          result.status === 'fulfilled' ? [result.value.data] : [],
        );
        const observations = results.flatMap((result) =>
          result.status === 'fulfilled' ? [result.value.meta] : [],
        );
        const newest = Math.max(0, ...observations.map((item) => Date.parse(item.observedAt)));
        const meta = responseMeta(new Date(newest || Date.now()));
        meta.freshnessMs = newest ? Date.now() - newest : 0;
        meta.partial = results.some(
          (result) => result.status === 'rejected' || result.value.meta.partial,
        );
        meta.warnings = [
          ...names
            .filter((_, index) => results[index]?.status === 'rejected')
            .map((name) => `${name}: unavailable`),
          ...observations.flatMap((item) => item.warnings),
        ];
        if (!closed)
          res.write(`event: queues\ndata: ${JSON.stringify({ success: true, data, meta })}\n\n`);
      } catch {
        if (!closed) res.write(': update unavailable\n\n');
      } finally {
        busy = false;
      }
    };
    void emit();
    const timer = setInterval(() => void emit(), Math.max(1_000, poller.intervalMs));
    timer.unref();
    res.once('close', close);
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
      return send(res, 200, { success: true, data: { status: 'ok' }, meta: responseMeta() });
    }
    if (config.token && !tokenMatches(req.headers.authorization, config.token)) {
      return error(res, 401, 'UNAUTHORIZED', 'Access token required');
    }

    try {
      if (path === '/api/v1/events') return streamSnapshots(req, res);
      if (
        path === '/api/v1/alerts/rules' ||
        path === '/api/v1/alerts/incidents' ||
        path === '/api/v1/alerts/notifications'
      ) {
        const allowed = await allowedQueues(req);
        if (config.registry.names().length && !allowed.length)
          return error(res, 403, 'FORBIDDEN', 'Access denied');
        const data = path.endsWith('/rules')
          ? (config.alertRules ?? []).filter((rule) => allowed.includes(rule.queue))
          : path.endsWith('/incidents')
            ? ((await config.alertStore?.listIncidents(100)) ?? []).filter((incident) =>
                allowed.includes(incident.queue),
              )
            : ((await config.alertStore?.listNotifications(100)) ?? []).filter((event) =>
                allowed.includes(event.queue),
              );
        return send(res, 200, { success: true, data, meta: responseMeta() });
      }
      if (path === '/api/v1/metrics') {
        const allowed = (await allowedQueues(req)).slice(0, 100);
        if (config.registry.names().length && !allowed.length)
          return error(res, 403, 'FORBIDDEN', 'Access denied');
        const observations = await Promise.allSettled(allowed.map((name) => poller.get(name)));
        const lines = [
          '# HELP queuelens_queue_jobs Observed BullMQ jobs by queue and state',
          '# TYPE queuelens_queue_jobs gauge',
          '# HELP queuelens_scrape_partial Whether any authorized queue is missing or stale',
          '# TYPE queuelens_scrape_partial gauge',
          '# HELP queuelens_queue_observation_age_seconds Age of the last queue observation',
          '# TYPE queuelens_queue_observation_age_seconds gauge',
        ];
        let partial = config.registry.names().length > 100;
        for (let index = 0; index < allowed.length; index++) {
          const result = observations[index];
          if (result?.status !== 'fulfilled') {
            partial = true;
            continue;
          }
          const label = JSON.stringify(allowed[index]);
          partial ||= result.value.meta.partial;
          lines.push(
            `queuelens_queue_observation_age_seconds{queue=${label}} ${Math.max(0, (Date.now() - Date.parse(result.value.meta.observedAt)) / 1000)}`,
          );
          for (const [state, value] of Object.entries(result.value.data.counts))
            lines.push(
              `queuelens_queue_jobs{queue=${label},state=${JSON.stringify(state)}} ${value}`,
            );
        }
        lines.push(`queuelens_scrape_partial ${partial ? 1 : 0}`);
        res.writeHead(200, {
          'content-type': 'text/plain; version=0.0.4; charset=utf-8',
          'cache-control': 'no-store',
        });
        res.end(`${lines.join('\n')}\n`);
        return;
      }
      if (path === '/api/v1/ready') {
        if (!(await permitted(req, { permission: 'queue:read' }))) {
          return error(res, 403, 'FORBIDDEN', 'Access denied');
        }
        const result = await poller.getAll();
        const meta = { ...result.meta, warnings: [] };
        if (result.meta.partial) {
          return error(res, 503, 'REDIS_UNAVAILABLE', 'Monitor is degraded', meta);
        }
        return send(res, 200, {
          success: true,
          data: { status: 'ready' },
          meta,
        });
      }
      if (path === '/api/v1/workers') {
        const names = config.registry.names();
        const allowed: string[] = [];
        for (const name of names) {
          if (await permitted(req, { queueName: name, permission: 'queue:read' }))
            allowed.push(name);
        }
        if (names.length > 0 && allowed.length === 0)
          return error(res, 403, 'FORBIDDEN', 'Access denied');
        const results = await Promise.allSettled(
          allowed.map((name) =>
            poller.runQueueRead(
              name,
              () => config.registry.get(name)?.listWorkers?.() ?? Promise.resolve([]),
            ),
          ),
        );
        const workers = results.flatMap((result) =>
          result.status === 'fulfilled' ? result.value : [],
        );
        const meta = responseMeta();
        meta.partial = results.some((result) => result.status === 'rejected');
        meta.warnings = allowed
          .filter((_, index) => results[index]?.status === 'rejected')
          .map((name) => `${name}: worker telemetry unavailable`);
        return send(res, 200, {
          success: true,
          data: {
            workers,
            uninstrumentedLiveness: 'unknown',
            message: 'Only instrumented workers report liveness; other workers remain unknown.',
          },
          meta,
        });
      }
      if (path === '/api/v1/queues') {
        const names = config.registry.names();
        const allowed: string[] = [];
        for (const name of names) {
          if (await permitted(req, { queueName: name, permission: 'queue:read' }))
            allowed.push(name);
        }
        if (names.length > 0 && allowed.length === 0) {
          return error(res, 403, 'FORBIDDEN', 'Access denied');
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
        return send(res, 200, { success: true, data: snapshots, meta });
      }

      const jobsMatch = /^\/api\/v1\/queues\/([^/]+)\/jobs$/.exec(path);
      const jobMatch = /^\/api\/v1\/queues\/([^/]+)\/jobs\/([^/]+)$/.exec(path);
      const flowMatch = /^\/api\/v1\/queues\/([^/]+)\/jobs\/([^/]+)\/flow$/.exec(path);
      const schedulersMatch = /^\/api\/v1\/queues\/([^/]+)\/schedulers$/.exec(path);
      const diagnosticsMatch = /^\/api\/v1\/queues\/([^/]+)\/diagnostics$/.exec(path);
      const historyMatch = /^\/api\/v1\/queues\/([^/]+)\/history$/.exec(path);
      const queueMatch = /^\/api\/v1\/queues\/([^/]+)$/.exec(path);
      const rawName =
        jobsMatch?.[1] ??
        jobMatch?.[1] ??
        flowMatch?.[1] ??
        schedulersMatch?.[1] ??
        diagnosticsMatch?.[1] ??
        historyMatch?.[1] ??
        queueMatch?.[1];
      if (!rawName) return error(res, 404, 'INVALID_REQUEST', 'Route not found');
      const name = segment(rawName);
      if (!name) return error(res, 400, 'INVALID_REQUEST', 'Invalid queue name');
      const queue = config.registry.get(name);
      if (!queue) return error(res, 404, 'QUEUE_NOT_FOUND', 'Queue not found');
      const permission = jobsMatch || jobMatch || flowMatch ? 'job:read' : 'queue:read';
      if (!(await permitted(req, { queueName: name, permission }))) {
        return error(res, 403, 'FORBIDDEN', 'Access denied');
      }

      if (queueMatch) {
        const snapshot = await poller.get(name);
        return send(res, 200, { success: true, ...snapshot });
      }
      if (historyMatch) {
        if (!config.history) return error(res, 404, 'INVALID_REQUEST', 'History is disabled');
        const range = url.searchParams.get('range') ?? '1h';
        const duration = (
          { '1h': 3_600_000, '6h': 21_600_000, '24h': 86_400_000 } as Record<string, number>
        )[range];
        if (!duration || duration > config.history.retentionMs)
          return error(res, 400, 'INVALID_FILTER', 'Invalid history range');
        const to = Date.now();
        const data = await config.history.read(name, to - duration, to);
        const meta = responseMeta();
        meta.partial = data.partial;
        meta.warnings = data.warnings;
        return send(res, 200, { success: true, data, meta });
      }
      if (diagnosticsMatch) {
        const diagnostics = await sampleQueueDiagnostics(
          queue,
          (operation) => poller.runQueueRead(name, operation),
          poller.getTrend(name),
        );
        return send(res, 200, { success: true, data: diagnostics, meta: responseMeta() });
      }
      if (schedulersMatch) {
        const schedulers = await poller.runQueueRead(
          name,
          () => queue.listSchedulers?.() ?? Promise.resolve([]),
        );
        const meta = responseMeta();
        meta.partial = schedulers.length === 50;
        if (meta.partial) meta.warnings.push('Only the first 50 schedulers are shown');
        return send(res, 200, {
          success: true,
          data: schedulers.map((item) => ({
            key: item.key,
            name: item.name,
            next: item.next,
            every: item.every,
            pattern: item.pattern,
            iterationCount: item.iterationCount,
            limit: item.limit,
            endDate: item.endDate,
            timeZone: item.timeZone,
          })),
          meta,
        });
      }
      if (flowMatch?.[2]) {
        const id = segment(flowMatch[2]);
        if (!id) return error(res, 400, 'INVALID_REQUEST', 'Invalid job ID');
        const flow = await poller.runQueueRead(
          name,
          () => queue.getFlow?.(id) ?? Promise.resolve(null),
        );
        if (!flow) return error(res, 404, 'JOB_NOT_FOUND', 'Job not found');
        const linked = async (key: string) => {
          for (const candidate of config.registry.names().sort((a, b) => b.length - a.length)) {
            const marker = `:${candidate}:`;
            const position = key.lastIndexOf(marker);
            if (position < 0) continue;
            const jobId = key.slice(position + marker.length);
            if (!jobId || jobId.length > 128) continue;
            if (await permitted(req, { queueName: candidate, permission: 'job:read' }))
              return { queue: candidate, id: jobId };
          }
          return null;
        };
        const parent = flow.parent
          ? await linked(`${flow.parent.queueKey}:${flow.parent.id}`)
          : null;
        const children = (await Promise.all(flow.childKeys.map(linked))).filter(
          (item) => item !== null,
        );
        const meta = responseMeta();
        meta.partial =
          flow.partial ||
          children.length !== flow.childKeys.length ||
          Boolean(flow.parent && !parent);
        if (meta.partial)
          meta.warnings.push('Some flow links are truncated or outside your authorized queues');
        return send(res, 200, {
          success: true,
          data: { parent, children, source: 'bullmq-public-api' },
          meta,
        });
      }
      if (jobMatch?.[2]) {
        const id = segment(jobMatch[2]);
        if (!id) return error(res, 400, 'INVALID_REQUEST', 'Invalid job ID');
        const job = await poller.runQueueRead(name, () => queue.getJob(id));
        if (!job) return error(res, 404, 'JOB_NOT_FOUND', 'Job not found');
        return send(res, 200, {
          success: true,
          data: safeJobDetail(job, config.redaction),
          meta: responseMeta(),
        });
      }
      if (jobsMatch) {
        const state = url.searchParams.get('state') ?? 'failed';
        const limit = positiveInteger(url.searchParams.get('limit'), 50, 100);
        const offset = positiveInteger(url.searchParams.get('offset'), 0, 10_000);
        const from = timestamp(url.searchParams.get('from'));
        const to = timestamp(url.searchParams.get('to'));
        const filterName = url.searchParams.get('name');
        const attemptsMin = positiveInteger(url.searchParams.get('attemptsMin'), 0, 100);
        const categoryFilter = url.searchParams.get('errorCategory');
        if (
          !observedStates.includes(state as ObservedState) ||
          !limit ||
          offset === null ||
          from === null ||
          to === null ||
          (from && to && (from > to || to - from > 7 * 24 * 60 * 60 * 1_000)) ||
          (filterName && filterName.length > 128) ||
          attemptsMin === null ||
          (categoryFilter !== null && state !== 'failed') ||
          (categoryFilter !== null &&
            !['timeout', 'rate_limit', 'validation', 'other', 'unknown'].includes(categoryFilter))
        ) {
          return error(res, 400, 'INVALID_FILTER', 'Invalid job filter');
        }
        const filtered = Boolean(filterName || from || to || attemptsMin || categoryFilter);
        if (filtered && offset > 500)
          return error(res, 400, 'INVALID_FILTER', 'Filtered offset exceeds 500');
        const scanLimit = filtered ? 500 : limit;
        const raw = await poller.runQueueRead(name, () =>
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
                (!to || job.timestamp <= to) &&
                (!attemptsMin || job.attemptsMade >= attemptsMin) &&
                (!categoryFilter || errorCategory(job.failedReason) === categoryFilter),
            )
          : raw;
        const page = filtered ? matches.slice(offset, offset + limit) : matches;
        const meta = responseMeta();
        meta.partial = filtered && raw.length === scanLimit;
        if (meta.partial)
          meta.warnings.push('Filtering inspected only the 500 newest jobs in this state');
        if (offset > 0) meta.warnings.push('Offset pages can shift while jobs change state');
        return send(res, 200, {
          success: true,
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
