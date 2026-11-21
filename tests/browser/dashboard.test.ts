import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { test } from '@playwright/test';

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

test('browser shows queues and sanitized job detail; token stays in request header', async ({
  page,
}) => {
  const requested: string[] = [];
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/v1/')) {
      requested.push(route.request().headers().authorization ?? '');
      const data = url.pathname.endsWith('/jobs/failed-1')
        ? {
            ...job,
            payload: { visibility: 'hidden', reason: 'policy' },
            returnValue: { visibility: 'hidden', reason: 'policy' },
            failureReason: 'synthetic failure',
            stacktrace: [],
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
      await route.fulfill({ json: { data, meta } });
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
  await page.keyboard.press('Escape');
  await page.getByRole('heading', { name: 'Payload' }).waitFor({ state: 'detached' });
  assert.ok(requested.length >= 3);
  assert.ok(requested.every((header) => header === 'Bearer browser-token'));
});

test('dashboard fits a mobile viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route('http://queuelens.test/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/v1/')) {
      const data = url.pathname.endsWith('/jobs')
        ? { jobs: [], nextOffset: null }
        : [
            {
              name: 'email',
              counts: { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 },
              paused: false,
            },
          ];
      await route.fulfill({ json: { data, meta } });
      return;
    }
    const path = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    await route.fulfill({ path: resolve(dist, path) });
  });
  await page.goto('http://queuelens.test/');
  await page.getByLabel('Access token').fill('browser-token');
  await page.getByRole('button', { name: 'Open monitor' }).click();
  await page.getByRole('heading', { name: 'Jobs' }).waitFor();
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth,
  );
  assert.equal(overflow, false);
  await page.getByRole('button', { name: 'Disconnect' }).first().click();
  await page.getByLabel('Access token').waitFor();
});
