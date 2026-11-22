import { sampleQueueDiagnostics } from '../../core/src/diagnostics.js';
import type { HistorySample } from '../../core/src/history.js';
import type { QueueRegistry } from '../../core/src/monitor.js';
import type { SnapshotPoller } from '../../core/src/poller.js';
import type { RedisHistoryStore } from './history-store.js';

export class HistoryCollector {
  private timer: NodeJS.Timeout | undefined;
  private stopped = false;
  private running = false;

  constructor(
    private readonly registry: QueueRegistry,
    private readonly poller: SnapshotPoller,
    private readonly store: RedisHistoryStore,
    private readonly onSample?: (sample: HistorySample) => Promise<void>,
  ) {}

  start(): void {
    if (this.timer || this.stopped) return;
    const schedule = (delay: number) => {
      this.timer = setTimeout(() => {
        void this.collect().finally(() => {
          if (!this.stopped) schedule(this.store.collectionIntervalMs);
        });
      }, delay);
      this.timer.unref();
    };
    schedule(Math.min(1_000, this.store.collectionIntervalMs));
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }

  async collect(): Promise<void> {
    if (this.running || this.stopped) return;
    this.running = true;
    try {
      for (const name of this.registry.names()) {
        if (this.stopped) break;
        const queue = this.registry.get(name);
        if (!queue) continue;
        try {
          const observed = await this.poller.get(name);
          if (observed.meta.partial) continue;
          let completedPerMinute: number | null = null;
          let failedPerMinute: number | null = null;
          let rateSampleSize = 0;
          try {
            const diagnostics = await sampleQueueDiagnostics(
              queue,
              (read) => this.poller.runQueueRead(name, read),
              [],
            );
            completedPerMinute = diagnostics.completedPerMinute;
            failedPerMinute = diagnostics.failedPerMinute;
            rateSampleSize = diagnostics.completed + diagnostics.failed;
          } catch {
            // Counts remain useful even if bounded diagnostic reads are unavailable.
          }
          const snapshot = observed.data;
          const at = Date.parse(observed.meta.observedAt);
          const sample: HistorySample = {
            queue: name,
            observedAt: at,
            counts: snapshot.counts,
            paused: Boolean(snapshot.paused),
            oldestWaitingAgeMs: snapshot.oldestWaitingAt
              ? Math.max(0, at - snapshot.oldestWaitingAt)
              : null,
            completedPerMinute,
            failedPerMinute,
            rateSampleSize,
            source: 'sampled',
          };
          if ((await this.store.append(sample)) && this.onSample)
            void this.onSample(sample).catch(() => undefined);
        } catch {
          // A missed collection period becomes an explicit gap in history.
        }
      }
    } finally {
      this.running = false;
    }
  }
}
