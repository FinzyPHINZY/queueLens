import { Redis } from 'ioredis';
import { redisOptions } from '../../bullmq-adapter/src/connection.js';

export interface CancellationProbeOptions {
  redisUrl: string;
  keyPrefix?: string;
}

/** Processors opt in to polling this advisory flag and decide when to stop safely. */
export function createCancellationProbe(options: CancellationProbeOptions) {
  const prefix = options.keyPrefix ?? 'queuelens:admin:v1';
  if (!/^[A-Za-z0-9:_-]{1,80}$/.test(prefix)) throw new Error('Invalid audit key prefix');
  const redis = new Redis({
    ...redisOptions(options.redisUrl),
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    connectTimeout: 2_000,
  });
  return {
    async isRequested(queue: string, jobId: string): Promise<boolean> {
      if (redis.status === 'connecting')
        await new Promise<void>((resolve, reject) => {
          const finish = (cause?: Error) => {
            clearTimeout(timer);
            redis.off('ready', onReady);
            redis.off('error', onError);
            if (cause) reject(cause);
            else resolve();
          };
          const onReady = () => finish();
          const onError = () => finish(new Error('Cancellation probe unavailable'));
          const timer = setTimeout(
            () => finish(new Error('Cancellation probe unavailable')),
            2_000,
          );
          redis.once('ready', onReady);
          redis.once('error', onError);
          if (redis.status === 'ready') finish();
        });
      if (redis.status !== 'ready') throw new Error('Cancellation probe unavailable');
      return (
        (await redis.exists(`${prefix}:cancel:${encodeURIComponent(`${queue}:${jobId}`)}`)) === 1
      );
    },
    async close(): Promise<void> {
      redis.disconnect();
    },
  };
}
