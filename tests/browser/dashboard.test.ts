import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { expect, test } from '@playwright/test';

const dist = resolve('apps/dashboard/dist');
const meta = {
  observedAt: new Date().toISOString(),
  source: 'bullmq',
  freshnessMs: 0,
  partial: false,
  warnings: [],
};
const job = {
  id: 'failed-1',
  name: 'email',
  state: 'failed',
  attemptsMade: 2,
  timestamp: Date.now(),
};
const diagnostics = {
  source: 'sampled',
  windowMs: 300000,
  completed: 0,
  failed: 0,
  completedPerMinute: 0,
  failedPerMinute: 0,
  retried: 0,
  errorCategories: {},
  wait: { sampleSize: 0, p50Ms: null, p95Ms: null, p99Ms: null },
  processing: { sampleSize: 0, p50Ms: null, p95Ms: null, p99Ms: null },
  trend: [],
  outcomeTrend: [],
  limitations: ['Sampled retained jobs only.'],
};

test('browser shows queues and sanitized job detail; token stays in request header', async ({
  page,
}) => {
  const requested: string[] = [];
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/v1/')) {
      if (!route.request().headers().authorization) {
        await route.fulfill({
          status: 401,
          json: {
            success: false,
            data: null,
            meta,
            error: { code: 'UNAUTHORIZED', message: 'Access token required' },
          },
        });
        return;
      }
      requested.push(route.request().headers().authorization ?? '');
      const data = url.pathname.endsWith('/flow')
        ? { parent: null, children: [], source: 'bullmq-public-api' }
        : url.pathname.endsWith('/workers')
          ? { workers: [], uninstrumentedLiveness: 'unknown' }
          : url.pathname.endsWith('/diagnostics')
            ? diagnostics
            : url.pathname.endsWith('/schedulers')
              ? []
              : url.pathname.endsWith('/jobs/failed-1')
                ? {
                    ...job,
                    payload: { visibility: 'hidden', reason: 'policy' },
                    returnValue: { visibility: 'hidden', reason: 'policy' },
                    failureReason: 'synthetic failure',
                    stacktrace: [],
                    timeline: [],
                  }
                : url.pathname.endsWith('/jobs')
                  ? { jobs: [job], nextOffset: null }
                  : [
                      {
                        name: 'email',
                        counts: { waiting: 1, active: 0, delayed: 0, completed: 0, failed: 1 },
                        paused: false,
                      },
                    ];
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    const path = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    assert.match(path, /^(index\.html|assets\/[\w.-]+)$/);
    const contents = await readFile(resolve(dist, path));
    await route.fulfill({
      body: contents,
      contentType: path.endsWith('.js')
        ? 'text/javascript'
        : path.endsWith('.css')
          ? 'text/css'
          : 'text/html',
    });
  });
  await page.goto('http://queuelens.test/');
  await page.getByLabel('Access token').fill('browser-token');
  await page.getByRole('button', { name: 'Open monitor' }).click();
  await page.getByRole('heading', { name: 'email', exact: true }).waitFor();
  await page.getByRole('button', { name: 'Inspect' }).click();
  await page.getByRole('heading', { name: 'Payload' }).waitFor();
  await page.getByText('Hidden (policy) by monitor policy.').first().waitFor();
  await expect(page.getByRole('button', { name: 'Close inspector' })).toBeFocused();
  await page.keyboard.press('Shift+Tab');
  await expect(page.getByRole('button', { name: 'Close inspector' })).toBeFocused();
  await page.screenshot({ path: 'test-results/dashboard-desktop.png', fullPage: true });
  await page.keyboard.press('Escape');
  await page.getByRole('heading', { name: 'Payload' }).waitFor({ state: 'detached' });
  await page.getByRole('button', { name: /Workers/ }).click();
  await page.getByRole('heading', { name: 'Worker health' }).waitFor();
  await page.getByText('Uninstrumented worker liveness is unknown.').waitFor();
  await page.getByRole('button', { name: 'Switch to light theme' }).click();
  assert.equal(await page.locator('.app.light').count(), 1);
  await page.screenshot({ path: 'test-results/dashboard-workers-light.png', fullPage: true });
  assert.ok(requested.length >= 3);
  assert.ok(requested.every((header) => header === 'Bearer browser-token'));
});

test('dashboard fits a mobile viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/v1/')) {
      if (!route.request().headers().authorization) {
        await route.fulfill({
          status: 401,
          json: {
            success: false,
            data: null,
            meta,
            error: { code: 'UNAUTHORIZED', message: 'Access token required' },
          },
        });
        return;
      }
      const data = url.pathname.endsWith('/workers')
        ? { workers: [], uninstrumentedLiveness: 'unknown' }
        : url.pathname.endsWith('/diagnostics')
          ? diagnostics
          : url.pathname.endsWith('/schedulers')
            ? []
            : url.pathname.endsWith('/jobs')
              ? { jobs: [], nextOffset: null }
              : [
                  {
                    name: 'email',
                    counts: { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 },
                    paused: false,
                  },
                ];
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    const path = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    await route.fulfill({ path: resolve(dist, path) });
  });
  await page.goto('http://queuelens.test/');
  await page.getByLabel('Access token').fill('browser-token');
  await page.getByRole('button', { name: 'Open monitor' }).click();
  await page.getByRole('heading', { name: 'Jobs' }).waitFor();
  await page.screenshot({ path: 'test-results/dashboard-mobile.png', fullPage: true });
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth,
  );
  assert.equal(overflow, false);
  await page.getByRole('button', { name: 'Disconnect' }).first().click();
  await page.getByLabel('Access token').waitFor();
});

test('session-authorized embedded dashboard opens without a token prompt', async ({ page }) => {
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/monitor/api/v1/')) {
      const data = url.pathname.endsWith('/workers')
        ? { workers: [], uninstrumentedLiveness: 'unknown' }
        : url.pathname.endsWith('/diagnostics')
          ? diagnostics
          : url.pathname.endsWith('/schedulers')
            ? []
            : url.pathname.endsWith('/jobs')
              ? { jobs: [], nextOffset: null }
              : [
                  {
                    name: 'email',
                    counts: { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 },
                    paused: false,
                  },
                ];
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    const path =
      url.pathname === '/monitor/' ? 'index.html' : url.pathname.replace('/monitor/', '');
    await route.fulfill({ path: resolve(dist, path) });
  });
  await page.goto('http://queuelens.test/monitor/');
  await page.getByRole('heading', { name: 'Jobs' }).waitFor();
  assert.equal(await page.getByLabel('Access token').count(), 0);
});

test('live queue stream reconnects after interruption while polling remains available', async ({
  page,
}) => {
  let connections = 0;
  const queues = [
    {
      name: 'email',
      counts: { waiting: 1, active: 0, delayed: 0, completed: 0, failed: 0 },
      paused: false,
    },
  ];
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/v1/events') {
      connections++;
      await route.fulfill({
        contentType: 'text/event-stream',
        body: `event: queues\ndata: ${JSON.stringify({ success: true, data: queues, meta })}\n\n`,
      });
      return;
    }
    if (url.pathname.startsWith('/api/v1/')) {
      if (!route.request().headers().authorization) {
        await route.fulfill({
          status: 401,
          json: {
            success: false,
            data: null,
            meta,
            error: { code: 'UNAUTHORIZED', message: 'Access token required' },
          },
        });
        return;
      }
      const data = url.pathname.endsWith('/workers')
        ? { workers: [], uninstrumentedLiveness: 'unknown' }
        : url.pathname.endsWith('/diagnostics')
          ? diagnostics
          : url.pathname.endsWith('/schedulers')
            ? []
            : url.pathname.endsWith('/jobs')
              ? { jobs: [], nextOffset: null }
              : queues;
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    const path = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    await route.fulfill({ path: resolve(dist, path) });
  });
  await page.goto('http://queuelens.test/');
  await page.getByLabel('Access token').fill('browser-token');
  await page.getByRole('button', { name: 'Open monitor' }).click();
  await page.getByText('Polling fallback', { exact: false }).waitFor();
  await expect.poll(() => connections, { timeout: 8_000 }).toBeGreaterThan(1);
  await page.getByRole('heading', { name: 'Jobs' }).waitFor();
});

test('history shows sampling gaps and alerts show incident delivery', async ({ page }) => {
  const queue = {
    name: 'email',
    counts: { waiting: 2, active: 0, delayed: 0, completed: 0, failed: 1 },
    paused: false,
  };
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/v1/')) {
      if (!route.request().headers().authorization) {
        await route.fulfill({
          status: 401,
          json: {
            success: false,
            data: null,
            meta,
            error: { code: 'UNAUTHORIZED', message: 'Access token required' },
          },
        });
        return;
      }
      const data = url.pathname.endsWith('/history')
        ? {
            collectionIntervalMs: 60_000,
            retentionMs: 86_400_000,
            slots: [
              { at: 60_000, gap: false, sample: { counts: { waiting: 2 } } },
              { at: 120_000, gap: true, sample: null },
              { at: 180_000, gap: false, sample: { counts: { waiting: 3 } } },
            ],
            warnings: ['Missing collection periods are shown as gaps'],
          }
        : url.pathname.endsWith('/alerts/rules')
          ? [
              {
                id: 'email-backlog',
                queue: 'email',
                metric: 'backlog',
                threshold: 2,
                sustainMs: 120_000,
                cooldownMs: 300_000,
                channel: 'webhook',
                enabled: true,
              },
            ]
          : url.pathname.endsWith('/alerts/incidents')
            ? [
                {
                  id: 'email-backlog:1',
                  ruleId: 'email-backlog',
                  queue: 'email',
                  metric: 'backlog',
                  status: 'open',
                  openedAt: Date.now(),
                  lastValue: 3,
                },
              ]
            : url.pathname.endsWith('/alerts/notifications')
              ? [
                  {
                    id: 'email-backlog:1:opening',
                    queue: 'email',
                    kind: 'opening',
                    channel: 'webhook',
                    status: 'sent',
                    attempts: 1,
                  },
                ]
              : url.pathname.endsWith('/workers')
                ? { workers: [], uninstrumentedLiveness: 'unknown' }
                : url.pathname.endsWith('/diagnostics')
                  ? diagnostics
                  : url.pathname.endsWith('/schedulers')
                    ? []
                    : url.pathname.endsWith('/jobs')
                      ? { jobs: [], nextOffset: null }
                      : [queue];
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    const path = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    await route.fulfill({ path: resolve(dist, path) });
  });
  await page.goto('http://queuelens.test/');
  await page.getByLabel('Access token').fill('browser-token');
  await page.getByRole('button', { name: 'Open monitor' }).click();
  await page.getByRole('button', { name: /History/ }).click();
  await page.getByRole('heading', { name: 'Queue history' }).waitFor();
  await page.getByRole('heading', { name: 'Failure rate' }).waitFor();
  await page.getByText('1 missing periods', { exact: false }).waitFor();
  await page.screenshot({ path: 'test-results/dashboard-history.png', fullPage: true });
  await page.getByRole('button', { name: /Alerts/ }).click();
  await page.getByText('email-backlog:1:opening').waitFor();
  await page.screenshot({ path: 'test-results/dashboard-alerts.png', fullPage: true });
});

test('management requires admin token and explicit confirmation', async ({ page }) => {
  let mutations = 0;
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/v1/admin/')) {
      if (route.request().headers().authorization !== 'Bearer admin-token') {
        await route.fulfill({
          status: 403,
          json: {
            success: false,
            data: null,
            meta,
            error: { code: 'FORBIDDEN', message: 'Access denied' },
          },
        });
        return;
      }
      const data = url.pathname.endsWith('/capabilities')
        ? { enabled: true, queues: [{ name: 'email', actions: ['retry', 'pause'] }] }
        : url.pathname.endsWith('/audit')
          ? []
          : { operationId: 'operation-1', action: 'retry', queue: 'email', status: 'succeeded' };
      if (route.request().method() === 'POST') {
        assert.ok(route.request().headers()['idempotency-key']);
        mutations++;
      }
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    if (url.pathname.startsWith('/api/v1/')) {
      if (!route.request().headers().authorization) {
        await route.fulfill({
          status: 401,
          json: {
            success: false,
            data: null,
            meta,
            error: { code: 'UNAUTHORIZED', message: 'Access token required' },
          },
        });
        return;
      }
      const data = url.pathname.endsWith('/workers')
        ? { workers: [], uninstrumentedLiveness: 'unknown' }
        : url.pathname.endsWith('/diagnostics')
          ? diagnostics
          : url.pathname.endsWith('/schedulers')
            ? []
            : url.pathname.endsWith('/jobs')
              ? { jobs: [], nextOffset: null }
              : [
                  {
                    name: 'email',
                    counts: { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 1 },
                    paused: false,
                  },
                ];
      await route.fulfill({ json: { success: true, data, meta } });
      return;
    }
    const path = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    await route.fulfill({ path: resolve(dist, path) });
  });
  await page.goto('http://queuelens.test/');
  await page.getByLabel('Access token').fill('read-token');
  await page.getByRole('button', { name: 'Open monitor' }).click();
  await page.getByRole('button', { name: /Manage/ }).click();
  await page.getByText('Enter an authorized admin token').waitFor();
  assert.equal(mutations, 0);
  await page.getByLabel('Admin token').fill('admin-token');
  await page.getByRole('button', { name: 'Unlock management' }).click();
  await page.getByRole('heading', { name: 'Action' }).waitFor();
  await page.getByLabel('Job ID').fill('failed-1');
  await page.getByRole('button', { name: 'Review action' }).click();
  assert.equal(mutations, 0);
  await page.getByRole('button', { name: 'Confirm retry' }).click();
  await page.getByText('succeeded: retry', { exact: false }).waitFor();
  assert.equal(mutations, 1);
  await page.screenshot({ path: 'test-results/dashboard-management.png', fullPage: true });
});
