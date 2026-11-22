import type { QueueCounts } from './monitor.js';

export interface HistorySample {
  queue: string;
  observedAt: number;
  counts: QueueCounts;
  paused: boolean;
  oldestWaitingAgeMs: number | null;
  completedPerMinute: number | null;
  failedPerMinute: number | null;
  rateSampleSize: number;
  source: 'sampled';
}

export interface HistorySlot {
  at: number;
  sample: HistorySample | null;
  gap: boolean;
}

export interface HistoryResult {
  from: number;
  to: number;
  stepMs: number;
  collectionIntervalMs: number;
  retentionMs: number;
  slots: HistorySlot[];
  source: 'sampled';
  partial: boolean;
  warnings: string[];
}

export interface HistoryReader {
  readonly collectionIntervalMs: number;
  readonly retentionMs: number;
  read(queue: string, from: number, to: number): Promise<HistoryResult>;
}
