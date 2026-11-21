import type { RedisOptions } from 'ioredis';

export function redisOptions(rawUrl: string): RedisOptions {
  const url = new URL(rawUrl);
  if (url.protocol !== 'redis:' && url.protocol !== 'rediss:') {
    throw new Error('REDIS_URL must use redis:// or rediss://');
  }
  const db = url.pathname && url.pathname !== '/' ? Number(url.pathname.slice(1)) : 0;
  const port = url.port ? Number(url.port) : 6379;
  if (!Number.isInteger(db) || db < 0 || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('REDIS_URL has an invalid database or port');
  }
  return {
    host: url.hostname,
    port,
    db,
    username: url.username ? decodeURIComponent(url.username) : undefined,
    password: url.password ? decodeURIComponent(url.password) : undefined,
    tls: url.protocol === 'rediss:' ? {} : undefined,
  };
}
