import type { RawJob, ReadOnlyQueue } from './monitor.js';
import type { TrendPoint } from './poller.js';

export interface Distribution {
  sampleSize: number;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
}

export interface QueueDiagnostics {
  windowMs: number;
  sampleLimitPerState: number;
  source: 'sampled';
  completed: number;
  failed: number;
  completedPerMinute: number;
  failedPerMinute: number;
  retried: number;
  errorCategories: Record<string, number>;
  wait: Distribution;
  processing: Distribution;
  trend: TrendPoint[];
  outcomeTrend: {
    startsAt: string;
    completed: number;
    failed: number;
    retried: number;
    source: 'sampled';
  }[];
  limitations: string[];
}

function distribution(values: number[]): Distribution {
  const sorted = values
    .filter((value) => Number.isFinite(value) && value >= 0)
    .sort((a, b) => a - b);
  const percentile = (fraction: number, minimum: number) =>
    sorted.length >= minimum ? (sorted[Math.ceil(fraction * sorted.length) - 1] ?? null) : null;
  return {
    sampleSize: sorted.length,
    p50Ms: percentile(0.5, 2),
    p95Ms: percentile(0.95, 20),
    p99Ms: percentile(0.99, 100),
  };
}

export function errorCategory(reason: string | undefined): string {
  if (!reason) return 'unknown';
  if (/timeout|timed out/i.test(reason)) return 'timeout';
  if (/rate.?limit|429/i.test(reason)) return 'rate_limit';
  if (/validat|invalid/i.test(reason)) return 'validation';
  return 'other';
}

export async function sampleQueueDiagnostics(
  queue: ReadOnlyQueue,
  runRead: <T>(operation: () => Promise<T>) => Promise<T>,
  trend: TrendPoint[],
  now = Date.now(),
): Promise<QueueDiagnostics> {
  const windowMs = 5 * 60_000;
  const sampleLimitPerState = 100;
  const completedJobs = await runRead(() =>
    queue.listJobs({ state: 'completed', offset: 0, limit: sampleLimitPerState }),
  );
  const failedJobs = await runRead(() =>
    queue.listJobs({ state: 'failed', offset: 0, limit: sampleLimitPerState }),
  );
  const inWindow = (job: RawJob) =>
    job.finishedOn !== undefined && job.finishedOn >= now - windowMs && job.finishedOn <= now;
  const completed = completedJobs.filter(inWindow);
  const failed = failedJobs.filter(inWindow);
  const jobs = [...completed, ...failed];
  const errorCategories: Record<string, number> = {};
  for (const job of failed) {
    const key = errorCategory(job.failedReason);
    errorCategories[key] = (errorCategories[key] ?? 0) + 1;
  }
  const values = (
    start: (job: RawJob) => number | undefined,
    end: (job: RawJob) => number | undefined,
  ) =>
    jobs.flatMap((job) => {
      const from = start(job);
      const to = end(job);
      return from !== undefined && to !== undefined && to >= from ? [to - from] : [];
    });
  return {
    windowMs,
    sampleLimitPerState,
    source: 'sampled',
    completed: completed.length,
    failed: failed.length,
    completedPerMinute: completed.length / 5,
    failedPerMinute: failed.length / 5,
    retried: jobs.filter((job) => job.attemptsMade > 1).length,
    errorCategories,
    wait: distribution(
      values(
        (job) => job.timestamp,
        (job) => job.processedOn,
      ),
    ),
    processing: distribution(
      values(
        (job) => job.processedOn,
        (job) => job.finishedOn,
      ),
    ),
    trend,
    outcomeTrend: Array.from({ length: 5 }, (_, index) => {
      const start = now - windowMs + index * 60_000;
      const inMinute = (job: RawJob) =>
        job.finishedOn !== undefined && job.finishedOn >= start && job.finishedOn < start + 60_000;
      const completedInMinute = completed.filter(inMinute);
      const failedInMinute = failed.filter(inMinute);
      return {
        startsAt: new Date(start).toISOString(),
        completed: completedInMinute.length,
        failed: failedInMinute.length,
        retried: [...completedInMinute, ...failedInMinute].filter((job) => job.attemptsMade > 1)
          .length,
        source: 'sampled' as const,
      };
    }),
    limitations: [
      'Metrics inspect at most the newest 100 retained jobs per completed and failed state.',
      'Rates divide retained jobs in a five-minute window; removed jobs and collection gaps can lower results.',
      'Trend points are in-memory snapshots and reset when the monitor restarts.',
    ],
  };
}
