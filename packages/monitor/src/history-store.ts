import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { redisOptions } from '../../bullmq-adapter/src/connection.js';
import type { HistoryReader, HistoryResult, HistorySample } from '../../core/src/history.js';
import type {
  AlertIncident,
  AlertStorage,
  NotificationEvent,
  RuleState,
} from '../../core/src/alerts.js';

export interface HistoryStoreOptions {
  redisUrl: string;
  collectionIntervalMs?: number;
  retentionMs?: number;
  keyPrefix?: string;
}

export class RedisHistoryStore implements HistoryReader, AlertStorage {
  readonly collectionIntervalMs: number;
  readonly retentionMs: number;
  readonly prefix: string;
  private readonly redis: Redis;
  private readonly instanceId = randomUUID();

  constructor(options: HistoryStoreOptions) {
    this.collectionIntervalMs = options.collectionIntervalMs ?? 60_000;
    this.retentionMs = options.retentionMs ?? 24 * 60 * 60_000;
    this.prefix = options.keyPrefix ?? 'queuelens:v1';
    if (
      !Number.isInteger(this.collectionIntervalMs) ||
      this.collectionIntervalMs < 5_000 ||
      this.collectionIntervalMs > 3_600_000
    )
      throw new Error('History interval must be between 5000 and 3600000 ms');
    if (
      !Number.isInteger(this.retentionMs) ||
      this.retentionMs < 3_600_000 ||
      this.retentionMs > 30 * 24 * 60 * 60_000
    )
      throw new Error('History retention must be between 1 hour and 30 days');
    if (!/^[A-Za-z0-9:_-]{1,80}$/.test(this.prefix)) throw new Error('Invalid history key prefix');
    this.redis = new Redis({
      ...redisOptions(options.redisUrl),
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
      connectTimeout: 2_000,
      retryStrategy: (attempt) => Math.min(5_000, attempt * 500),
    });
  }

  private queueKey(queue: string): string {
    return `${this.prefix}:history:${encodeURIComponent(queue)}`;
  }

  private alertStateKey(ruleId: string): string {
    return `${this.prefix}:alert:state:${ruleId}`;
  }
  private incidentKey(id: string): string {
    return `${this.prefix}:alert:incident:${encodeURIComponent(id)}`;
  }
  private notificationKey(id: string): string {
    return `${this.prefix}:notify:event:${encodeURIComponent(id)}`;
  }
  private get expirySeconds(): number {
    return Math.ceil(this.retentionMs / 1_000);
  }

  async append(sample: HistorySample): Promise<boolean> {
    const bucket =
      Math.floor(sample.observedAt / this.collectionIntervalMs) * this.collectionIntervalMs;
    const index = this.queueKey(sample.queue);
    const lease = `${index}:lease:${bucket}`;
    const claimed = await this.redis.set(
      lease,
      this.instanceId,
      'PX',
      this.collectionIntervalMs * 2,
      'NX',
    );
    if (claimed !== 'OK') return false;
    const dataKey = `${index}:point:${bucket}`;
    const expiresSeconds = Math.ceil(this.retentionMs / 1_000);
    const safe: HistorySample = {
      queue: sample.queue,
      observedAt: sample.observedAt,
      counts: {
        waiting: sample.counts.waiting,
        active: sample.counts.active,
        delayed: sample.counts.delayed,
        completed: sample.counts.completed,
        failed: sample.counts.failed,
        paused: sample.counts.paused,
        'waiting-children': sample.counts['waiting-children'],
      },
      paused: sample.paused,
      oldestWaitingAgeMs: sample.oldestWaitingAgeMs,
      completedPerMinute: sample.completedPerMinute,
      failedPerMinute: sample.failedPerMinute,
      rateSampleSize: sample.rateSampleSize,
      source: 'sampled',
    };
    await this.redis
      .multi()
      .set(dataKey, JSON.stringify(safe), 'EX', expiresSeconds)
      .zadd(index, bucket, String(bucket))
      .zremrangebyscore(index, '-inf', bucket - this.retentionMs)
      .expire(index, expiresSeconds)
      .exec();
    const count = await this.redis.zcard(index);
    if (count > 10_000) await this.redis.zremrangebyrank(index, 0, count - 10_001);
    return true;
  }

  async read(queue: string, from: number, to: number): Promise<HistoryResult> {
    if (
      !Number.isSafeInteger(from) ||
      !Number.isSafeInteger(to) ||
      from >= to ||
      to - from > this.retentionMs ||
      to > Date.now() + 60_000
    )
      throw new Error('Invalid history range');
    const interval = this.collectionIntervalMs;
    const stepMs = Math.ceil((to - from) / 500 / interval) * interval || interval;
    const start = Math.ceil(from / stepMs) * stepMs;
    const index = this.queueKey(queue);
    const ids = await this.redis.zrangebyscore(index, from, to, 'LIMIT', 0, 10_001);
    const selected = new Map<number, number>();
    for (const id of ids) {
      const time = Number(id);
      if (!Number.isSafeInteger(time)) continue;
      const slot = Math.floor(time / stepMs) * stepMs;
      selected.set(slot, time);
    }
    const times = [...selected.values()];
    const values = times.length
      ? await this.redis.mget(times.map((time) => `${index}:point:${time}`))
      : [];
    const samples = new Map<number, HistorySample>();
    for (let i = 0; i < times.length; i++) {
      const raw = values[i];
      if (!raw) continue;
      try {
        const sample = JSON.parse(raw) as HistorySample;
        if (sample.queue === queue && sample.source === 'sampled')
          samples.set(Math.floor(times[i]! / stepMs) * stepMs, sample);
      } catch {
        // Missing or malformed monitor-owned data is displayed as a gap.
      }
    }
    const slots = [];
    for (let at = start; at < to && slots.length < 500; at += stepMs) {
      const sample = samples.get(at) ?? null;
      slots.push({ at, sample, gap: sample === null });
    }
    const partial = ids.length > 10_000 || slots.some((slot) => slot.gap);
    return {
      from,
      to,
      stepMs,
      collectionIntervalMs: interval,
      retentionMs: this.retentionMs,
      slots,
      source: 'sampled',
      partial,
      warnings: [
        ...(ids.length > 10_000 ? ['History index exceeded the 10000-point read cap'] : []),
        ...(slots.some((slot) => slot.gap) ? ['Missing collection periods are shown as gaps'] : []),
        ...(stepMs > interval ? ['Display points are downsampled to fit 500 slots'] : []),
      ],
    };
  }

  async lease(key: string, ttlMs: number): Promise<boolean> {
    const result = await this.redis.set(
      `${this.prefix}:lease:${key}`,
      this.instanceId,
      'PX',
      ttlMs,
      'NX',
    );
    return result === 'OK';
  }

  async loadRuleState(ruleId: string): Promise<RuleState> {
    const raw = await this.redis.get(this.alertStateKey(ruleId));
    return raw ? (JSON.parse(raw) as RuleState) : {};
  }

  async saveRuleState(ruleId: string, state: RuleState): Promise<void> {
    await this.redis.set(
      this.alertStateKey(ruleId),
      JSON.stringify(state),
      'EX',
      this.expirySeconds,
    );
  }

  async loadIncident(id: string): Promise<AlertIncident | null> {
    const raw = await this.redis.get(this.incidentKey(id));
    return raw ? (JSON.parse(raw) as AlertIncident) : null;
  }

  async saveIncident(incident: AlertIncident): Promise<void> {
    const index = `${this.prefix}:alert:incidents`;
    await this.redis
      .multi()
      .set(this.incidentKey(incident.id), JSON.stringify(incident), 'EX', this.expirySeconds)
      .zadd(index, incident.openedAt, incident.id)
      .expire(index, this.expirySeconds)
      .exec();
    const count = await this.redis.zcard(index);
    if (count > 1_000) await this.redis.zremrangebyrank(index, 0, count - 1_001);
  }

  async commitIncident(
    ruleId: string,
    state: RuleState,
    incident: AlertIncident,
    event: NotificationEvent,
  ): Promise<void> {
    const incidentIndex = `${this.prefix}:alert:incidents`;
    const notificationIndex = `${this.prefix}:notify:index`;
    const dueIndex = `${this.prefix}:notify:due`;
    await this.redis
      .multi()
      .set(this.alertStateKey(ruleId), JSON.stringify(state), 'EX', this.expirySeconds)
      .set(this.incidentKey(incident.id), JSON.stringify(incident), 'EX', this.expirySeconds)
      .zadd(incidentIndex, incident.openedAt, incident.id)
      .expire(incidentIndex, this.expirySeconds)
      .set(this.notificationKey(event.id), JSON.stringify(event), 'EX', this.expirySeconds)
      .zadd(notificationIndex, event.at, event.id)
      .expire(notificationIndex, this.expirySeconds)
      .zadd(dueIndex, event.nextRetryAt, event.id)
      .expire(dueIndex, this.expirySeconds)
      .exec();
    const incidentCount = await this.redis.zcard(incidentIndex);
    if (incidentCount > 1_000)
      await this.redis.zremrangebyrank(incidentIndex, 0, incidentCount - 1_001);
    const notificationCount = await this.redis.zcard(notificationIndex);
    if (notificationCount > 1_000)
      await this.redis.zremrangebyrank(notificationIndex, 0, notificationCount - 1_001);
  }

  async listIncidents(limit: number): Promise<AlertIncident[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100)
      throw new Error('Invalid incident page size');
    const ids = await this.redis.zrevrange(`${this.prefix}:alert:incidents`, 0, limit - 1);
    if (!ids.length) return [];
    const values = await this.redis.mget(ids.map((id) => this.incidentKey(id)));
    return values.flatMap((raw) => {
      if (!raw) return [];
      try {
        return [JSON.parse(raw) as AlertIncident];
      } catch {
        return [];
      }
    });
  }

  async enqueueNotification(event: NotificationEvent): Promise<void> {
    await this.redis
      .multi()
      .set(this.notificationKey(event.id), JSON.stringify(event), 'EX', this.expirySeconds)
      .zadd(`${this.prefix}:notify:due`, event.nextRetryAt, event.id)
      .expire(`${this.prefix}:notify:due`, this.expirySeconds)
      .zadd(`${this.prefix}:notify:index`, event.at, event.id)
      .expire(`${this.prefix}:notify:index`, this.expirySeconds)
      .exec();
    const count = await this.redis.zcard(`${this.prefix}:notify:index`);
    if (count > 1_000)
      await this.redis.zremrangebyrank(`${this.prefix}:notify:index`, 0, count - 1_001);
  }

  async listNotifications(limit: number): Promise<NotificationEvent[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100)
      throw new Error('Invalid notification page size');
    const ids = await this.redis.zrevrange(`${this.prefix}:notify:index`, 0, limit - 1);
    if (!ids.length) return [];
    const values = await this.redis.mget(ids.map((id) => this.notificationKey(id)));
    return values.flatMap((raw) => {
      if (!raw) return [];
      try {
        return [JSON.parse(raw) as NotificationEvent];
      } catch {
        return [];
      }
    });
  }

  async dueNotifications(now: number, limit: number): Promise<NotificationEvent[]> {
    const ids = await this.redis.zrangebyscore(
      `${this.prefix}:notify:due`,
      '-inf',
      now,
      'LIMIT',
      0,
      50,
    );
    if (!ids.length) return [];
    const values = await this.redis.mget(ids.map((id) => this.notificationKey(id)));
    const events: NotificationEvent[] = [];
    const stale: string[] = [];
    for (let index = 0; index < ids.length; index++) {
      const raw = values[index];
      if (!raw) {
        stale.push(ids[index]!);
        continue;
      }
      try {
        events.push(JSON.parse(raw) as NotificationEvent);
      } catch {
        stale.push(ids[index]!);
      }
    }
    if (stale.length) await this.redis.zrem(`${this.prefix}:notify:due`, ...stale);
    return events.slice(0, Math.min(50, limit));
  }

  async saveNotification(event: NotificationEvent): Promise<void> {
    await this.enqueueNotification(event);
  }

  async finishNotification(event: NotificationEvent): Promise<void> {
    await this.redis
      .multi()
      .set(this.notificationKey(event.id), JSON.stringify(event), 'EX', this.expirySeconds)
      .zrem(`${this.prefix}:notify:due`, event.id)
      .exec();
  }

  async claimDelivery(id: string, ttlMs: number): Promise<boolean> {
    return this.lease(`delivery:${encodeURIComponent(id)}`, ttlMs);
  }

  async rememberWorker(queue: string, id: string, lastHeartbeat: number): Promise<void> {
    await this.redis.set(
      `${this.prefix}:worker:last:${encodeURIComponent(queue)}:${id}`,
      String(lastHeartbeat),
      'EX',
      this.expirySeconds,
    );
  }

  async lastWorkerHeartbeat(queue: string, id: string): Promise<number | null> {
    const raw = await this.redis.get(
      `${this.prefix}:worker:last:${encodeURIComponent(queue)}:${id}`,
    );
    if (!raw) return null;
    const value = Number(raw);
    return Number.isFinite(value) ? value : null;
  }

  async close(): Promise<void> {
    this.redis.disconnect();
  }
}
