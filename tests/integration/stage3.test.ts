import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Redis } from 'ioredis';
import type { HistorySample } from '../../packages/core/src/history.js';
import { AlertEngine, type AlertRule } from '../../packages/core/src/alerts.js';
import { RedisHistoryStore } from '../../packages/monitor/src/history-store.js';

const url = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379';
const counts = {
  waiting: 3,
  active: 0,
  delayed: 0,
  completed: 0,
  failed: 0,
  paused: 0,
  'waiting-children': 0,
};

test('history uses owned Redis keys, bounds samples, marks gaps, and omits unexpected payload fields', async () => {
  const prefix = `queuelens:test:${process.pid}:${Date.now()}`;
  const store = new RedisHistoryStore({
    redisUrl: url,
    keyPrefix: prefix,
    collectionIntervalMs: 5_000,
    retentionMs: 3_600_000,
  });
  const redis = new Redis(url);
  const now = Math.floor(Date.now() / 5_000) * 5_000;
  const sample: HistorySample = {
    queue: 'demo',
    observedAt: now - 10_000,
    counts,
    paused: false,
    oldestWaitingAgeMs: 1200,
    completedPerMinute: 0,
    failedPerMinute: 0,
    rateSampleSize: 0,
    source: 'sampled',
  };
  try {
    assert.equal(await store.append({ ...sample, secret: 'never-persist' } as HistorySample), true);
    assert.equal(await store.append(sample), false);
    const raw = await redis.get(`${prefix}:history:demo:point:${sample.observedAt}`);
    assert.ok(raw);
    assert.doesNotMatch(raw, /never-persist/);
    const result = await store.read('demo', now - 20_000, now + 1);
    assert.ok(result.slots.some((slot) => slot.sample?.counts.waiting === 3));
    assert.ok(result.slots.some((slot) => slot.gap));
    assert.equal(result.partial, true);
    assert.ok(result.slots.length <= 500);
    assert.equal(await store.lease('test', 10_000), true);
    assert.equal(await store.lease('test', 10_000), false);
  } finally {
    const keys = await redis.keys(`${prefix}:*`);
    if (keys.length) await redis.del(...keys);
    await redis.quit();
    await store.close();
  }
});

test('incident and notification state persists across monitor instances', async () => {
  const prefix = `queuelens:test:restart:${process.pid}:${Date.now()}`;
  const first = new RedisHistoryStore({
    redisUrl: url,
    keyPrefix: prefix,
    collectionIntervalMs: 5_000,
  });
  const second = new RedisHistoryStore({
    redisUrl: url,
    keyPrefix: prefix,
    collectionIntervalMs: 5_000,
  });
  const redis = new Redis(url);
  const rule: AlertRule = {
    id: 'backlog',
    queue: 'demo',
    metric: 'backlog',
    threshold: 2,
    sustainMs: 5_000,
    cooldownMs: 5_000,
    channel: 'webhook',
    enabled: true,
  };
  const at = Math.floor(Date.now() / 5_000) * 5_000 - 20_000;
  const sample: HistorySample = {
    queue: 'demo',
    observedAt: at,
    counts,
    paused: false,
    oldestWaitingAgeMs: null,
    completedPerMinute: 0,
    failedPerMinute: 0,
    rateSampleSize: 0,
    source: 'sampled',
  };
  try {
    await new AlertEngine(first, [rule], 5_000).evaluate(sample);
    await new AlertEngine(second, [rule], 5_000).evaluate({ ...sample, observedAt: at + 5_000 });
    assert.equal((await first.listIncidents(10))[0]?.status, 'open');
    assert.equal((await second.listNotifications(10))[0]?.kind, 'opening');
    await new AlertEngine(second, [rule], 5_000).evaluate({
      ...sample,
      observedAt: at + 10_000,
      counts: { ...counts, waiting: 0 },
    });
    assert.equal((await first.listIncidents(10))[0]?.status, 'resolved');
    assert.deepEqual((await second.listNotifications(10)).map((event) => event.kind).sort(), [
      'opening',
      'recovery',
    ]);
  } finally {
    const keys = await redis.keys(`${prefix}:*`);
    if (keys.length) await redis.del(...keys);
    await redis.quit();
    await first.close();
    await second.close();
  }
});
