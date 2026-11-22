import { Redis } from 'ioredis';
import { redisOptions } from '../../bullmq-adapter/src/connection.js';
import type {
  AuditEvent,
  ManagementPreviewRecord,
  ManagementStore,
  StoredOperation,
} from '../../core/src/management.js';

export interface AuditStoreOptions {
  redisUrl: string;
  keyPrefix?: string;
  requireAof?: boolean;
}

const reserveScript = `
local existing = redis.call('GET', KEYS[1])
if existing then return existing end
redis.call('XADD', KEYS[2], '*', 'record', ARGV[2])
redis.call('SET', KEYS[1], ARGV[1], 'PX', 86400000)
return 'NEW'
`;
const finishScript = `
redis.call('XADD', KEYS[2], '*', 'record', ARGV[2])
redis.call('SET', KEYS[1], ARGV[1], 'PX', 86400000)
return 1
`;
const rateScript = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], 60000) end
return count
`;
const releaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`;
const renewScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0
`;

export class RedisAuditStore implements ManagementStore {
  private readonly redis: Redis;
  private readonly prefix: string;
  private readonly requireAof: boolean;
  private verified = false;

  constructor(options: AuditStoreOptions) {
    this.prefix = options.keyPrefix ?? 'queuelens:admin:v1';
    this.requireAof = options.requireAof ?? true;
    if (!/^[A-Za-z0-9:_-]{1,80}$/.test(this.prefix)) throw new Error('Invalid audit key prefix');
    this.redis = new Redis({
      ...redisOptions(options.redisUrl),
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
      connectTimeout: 2_000,
      retryStrategy: (attempt) => Math.min(5_000, attempt * 500),
    });
  }

  private async ready(): Promise<void> {
    if (this.redis.status === 'connecting') {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => finish(new Error('Audit storage connection timed out')),
          2_000,
        );
        const finish = (cause?: Error) => {
          clearTimeout(timer);
          this.redis.off('ready', onReady);
          this.redis.off('error', onError);
          if (cause) reject(cause);
          else resolve();
        };
        const onReady = () => finish();
        const onError = () => finish(new Error('Audit storage unavailable'));
        this.redis.once('ready', onReady);
        this.redis.once('error', onError);
        if (this.redis.status === 'ready') finish();
      });
    }
    if (this.redis.status !== 'ready') throw new Error('Audit storage unavailable');
    if (!this.verified && this.requireAof) {
      const info = await this.redis.info('persistence');
      if (!/^aof_enabled:1\r?$/m.test(info))
        throw new Error('Audit Redis requires AOF persistence');
      this.verified = true;
    }
  }

  private key(kind: string, id: string): string {
    return `${this.prefix}:${kind}:${encodeURIComponent(id)}`;
  }
  private get stream(): string {
    return `${this.prefix}:events`;
  }

  private async durable(): Promise<void> {
    if (!this.requireAof) return;
    const result = (await this.redis.call('WAITAOF', 1, 0, 2_000)) as number[];
    if (!Array.isArray(result) || result[0] !== 1) throw new Error('Audit fsync timed out');
  }

  async savePreview(token: string, preview: ManagementPreviewRecord): Promise<void> {
    await this.ready();
    await this.redis.set(
      this.key('preview', token),
      JSON.stringify(preview),
      'PX',
      Math.max(1, preview.expiresAt - Date.now()),
    );
  }

  async loadPreview(token: string): Promise<ManagementPreviewRecord | null> {
    await this.ready();
    const raw = await this.redis.get(this.key('preview', token));
    if (!raw) return null;
    try {
      return JSON.parse(raw) as ManagementPreviewRecord;
    } catch {
      return null;
    }
  }

  async reserve(
    key: string,
    fingerprint: string,
    intent: AuditEvent,
  ): Promise<'new' | StoredOperation> {
    await this.ready();
    const pending: StoredOperation = { fingerprint, status: 'pending' };
    const result = await this.redis.eval(
      reserveScript,
      2,
      this.key('idempotency', key),
      this.stream,
      JSON.stringify(pending),
      JSON.stringify(intent),
    );
    if (result === 'NEW') {
      await this.durable();
      return 'new';
    }
    return JSON.parse(String(result)) as StoredOperation;
  }

  async getOperation(key: string): Promise<StoredOperation | null> {
    await this.ready();
    const raw = await this.redis.get(this.key('idempotency', key));
    return raw ? (JSON.parse(raw) as StoredOperation) : null;
  }

  async finish(key: string, operation: StoredOperation, event: AuditEvent): Promise<void> {
    await this.ready();
    await this.redis.eval(
      finishScript,
      2,
      this.key('idempotency', key),
      this.stream,
      JSON.stringify(operation),
      JSON.stringify(event),
    );
    await this.durable();
  }

  async recent(limit: number): Promise<AuditEvent[]> {
    await this.ready();
    if (!Number.isInteger(limit) || limit < 1 || limit > 100)
      throw new Error('Invalid audit limit');
    const entries = await this.redis.xrevrange(this.stream, '+', '-', 'COUNT', limit);
    return entries.flatMap(([, fields]) => {
      const value = fields[fields.indexOf('record') + 1];
      if (!value) return [];
      try {
        return [JSON.parse(value) as AuditEvent];
      } catch {
        return [];
      }
    });
  }

  async countAttempt(scope: string, limit: number): Promise<boolean> {
    await this.ready();
    const count = Number(await this.redis.eval(rateScript, 1, this.key('rate', scope)));
    return count <= limit;
  }

  async acquireQueue(queue: string, owner: string, ttlMs: number): Promise<boolean> {
    await this.ready();
    return (await this.redis.set(this.key('lock', queue), owner, 'PX', ttlMs, 'NX')) === 'OK';
  }

  async renewQueue(queue: string, owner: string, ttlMs: number): Promise<boolean> {
    await this.ready();
    return (
      Number(await this.redis.eval(renewScript, 1, this.key('lock', queue), owner, ttlMs)) === 1
    );
  }

  async releaseQueue(queue: string, owner: string): Promise<void> {
    await this.ready();
    await this.redis.eval(releaseScript, 1, this.key('lock', queue), owner);
  }

  async requestCancellation(queue: string, jobId: string, ttlMs: number): Promise<void> {
    await this.ready();
    await this.redis.set(this.key('cancel', `${queue}:${jobId}`), 'requested', 'PX', ttlMs);
  }

  async cancellationRequested(queue: string, jobId: string): Promise<boolean> {
    await this.ready();
    return (await this.redis.exists(this.key('cancel', `${queue}:${jobId}`))) === 1;
  }

  async close(): Promise<void> {
    this.redis.disconnect();
  }
}
