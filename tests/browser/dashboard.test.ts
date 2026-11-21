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
