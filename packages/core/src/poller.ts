import {
  responseMeta,
  type QueueRegistry,
  type QueueSnapshot,
  type ResponseMeta,
} from './monitor.js';
import { ReadBudget, ReadBudgetExceededError } from './read-budget.js';

export interface PollingOptions {
  intervalMs?: number;
  maxConcurrentReads?: number;
  queryTimeoutMs?: number;
  maxReadsPerQueue?: number;
}

export interface ObservedSnapshot {
  data: QueueSnapshot;
  meta: ResponseMeta;
}

interface CachedSnapshot {
  data: QueueSnapshot;
  observedAt: number;
}

export interface TrendPoint {
  observedAt: string;
  gapBeforeMs?: number;
  waiting: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  source: 'sampled';
}

export class SnapshotPoller {
  readonly intervalMs: number;
  readonly queryTimeoutMs: number;
  readonly budget: ReadBudget;
  readonly maxReadsPerQueue: number;
  private readonly queueReads = new Map<string, number>();
  private readonly cache = new Map<string, CachedSnapshot>();
  private readonly trends = new Map<string, TrendPoint[]>();
  private readonly inFlight = new Map<string, Promise<CachedSnapshot>>();
  private readonly failureCount = new Map<string, number>();
  private readonly retryAfter = new Map<string, number>();
  private timer: NodeJS.Timeout | undefined;
  private stopped = false;

  constructor(
    private readonly registry: QueueRegistry,
    options: PollingOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? 5_000;
    this.queryTimeoutMs = options.queryTimeoutMs ?? 2_000;
    this.budget = new ReadBudget(options.maxConcurrentReads ?? 4, this.queryTimeoutMs);
    this.maxReadsPerQueue = options.maxReadsPerQueue ?? 2;
    if (
      !Number.isInteger(this.maxReadsPerQueue) ||
      this.maxReadsPerQueue < 1 ||
      this.maxReadsPerQueue > 8
    )
      throw new Error('maxReadsPerQueue must be between 1 and 8');
    if (!Number.isInteger(this.intervalMs) || this.intervalMs < 1_000 || this.intervalMs > 60_000)
      throw new Error('Invalid polling interval');
  }

  start(): void {
    if (this.timer || this.stopped) return;
    const tick = () => {
      void Promise.allSettled(this.registry.names().map((name) => this.refresh(name)));
      const jitter = Math.floor(Math.random() * Math.min(1_000, this.intervalMs / 5));
      this.timer = setTimeout(tick, this.intervalMs + jitter);
      this.timer.unref();
    };
    tick();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }

  async get(name: string): Promise<ObservedSnapshot> {
    if (!this.registry.get(name)) throw new Error('Queue not found');
    const cached = this.cache.get(name);
    const now = Date.now();
    if (cached && now - cached.observedAt < this.intervalMs) {
      return this.observed(cached, false);
    }
    if (!this.stopped && (this.retryAfter.get(name) ?? 0) <= now) {
      try {
        const updated = await this.refresh(name);
        return this.observed(updated, false);
      } catch {
        // A failed refresh can still leave a last-known snapshot usable.
      }
    }
    if (cached) return this.observed(cached, true);
    throw new Error('Queue data unavailable');
  }

  async getAll(): Promise<{ data: QueueSnapshot[]; meta: ResponseMeta }> {
    const names = this.registry.names();
    const results = await Promise.allSettled(names.map((name) => this.get(name)));
    const values = results.flatMap((result) =>
      result.status === 'fulfilled' ? [result.value] : [],
    );
    if (names.length > 0 && values.length === 0) throw new Error('All queues unavailable');
    const missing = names.filter((_, index) => results[index]?.status === 'rejected');
    const stale = values.filter((value) => value.meta.partial).map((value) => value.data.name);
    const newest = Math.max(0, ...values.map((value) => Date.parse(value.meta.observedAt)));
    return {
      data: values.map((value) => value.data),
      meta: {
        ...responseMeta(new Date(newest || Date.now())),
        freshnessMs: newest ? Date.now() - newest : 0,
        partial: missing.length > 0 || stale.length > 0,
        warnings: [
          ...missing.map((name) => `${name}: unavailable`),
          ...stale.map((name) => `${name}: showing stale data`),
        ],
      },
    };
  }

  getTrend(name: string): TrendPoint[] {
    return [...(this.trends.get(name) ?? [])];
  }

  runQueueRead<T>(name: string, operation: () => Promise<T>): Promise<T> {
    const active = this.queueReads.get(name) ?? 0;
    if (active >= this.maxReadsPerQueue) throw new ReadBudgetExceededError();
    this.queueReads.set(name, active + 1);
    const release = () => {
      const remaining = (this.queueReads.get(name) ?? 1) - 1;
      if (remaining) this.queueReads.set(name, remaining);
      else this.queueReads.delete(name);
    };
    try {
      return this.budget.run(async () => {
        try {
          return await operation();
        } finally {
          release();
        }
      });
    } catch (cause) {
      release();
      throw cause;
    }
  }

  private observed(cached: CachedSnapshot, stale: boolean): ObservedSnapshot {
    const meta = responseMeta(new Date(cached.observedAt));
    meta.freshnessMs = Math.max(0, Date.now() - cached.observedAt);
    meta.partial = stale;
    if (stale) meta.warnings.push('Redis data unavailable; showing last successful snapshot');
    return { data: cached.data, meta };
  }

  private refresh(name: string): Promise<CachedSnapshot> {
    const existing = this.inFlight.get(name);
    if (existing) return existing;
    const operation = (async () => {
      try {
        const data = await this.runQueueRead(name, () => this.registry.snapshot(name));
        if (!data) throw new Error('Queue not found');
        const cached = { data, observedAt: Date.now() };
        this.cache.set(name, cached);
        const points = this.trends.get(name) ?? [];
        const previous = points.at(-1);
        const gapBeforeMs = previous ? cached.observedAt - Date.parse(previous.observedAt) : 0;
        points.push({
          observedAt: new Date(cached.observedAt).toISOString(),
          gapBeforeMs: gapBeforeMs > this.intervalMs * 1.5 ? gapBeforeMs : undefined,
          waiting: data.counts.waiting,
          delayed: data.counts.delayed,
          active: data.counts.active,
          completed: data.counts.completed,
          failed: data.counts.failed,
          source: 'sampled',
        });
        this.trends.set(name, points.slice(-60));
        this.failureCount.delete(name);
        this.retryAfter.delete(name);
        return cached;
      } catch {
        const failures = (this.failureCount.get(name) ?? 0) + 1;
        this.failureCount.set(name, failures);
        this.retryAfter.set(name, Date.now() + Math.min(30_000, 500 * 2 ** Math.min(failures, 6)));
        throw new Error('Queue data unavailable');
      }
    })();
    this.inFlight.set(name, operation);
    void operation.finally(() => this.inFlight.delete(name)).catch(() => undefined);
    return operation;
  }
}
