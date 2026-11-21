import type { JobMetadata, RawJob } from './monitor.js';

export interface DisclosurePolicy {
  payloadAccess?: 'deny' | 'allow';
  allowedPaths?: string[];
  redactPaths?: string[];
  showFailureDetails?: boolean;
  maxValueBytes?: number;
}

export type DisclosedValue =
  | { visibility: 'hidden'; reason: 'policy' | 'size' | 'unavailable' }
  | { visibility: 'visible' | 'partial'; value: unknown };

export interface SafeJobDetail extends JobMetadata {
  payload: DisclosedValue;
  returnValue: DisclosedValue;
  failureReason: string;
  stacktrace: string[];
}

const OMIT = Symbol('omit');
const MAX_TEXT_LENGTH = 4_000;

function parsePointer(pointer: string): string[] {
  if (!pointer.startsWith('/') || /~(?![01])/.test(pointer)) {
    throw new Error('Redaction paths must use JSON Pointer syntax');
  }
  return pointer
    .slice(1)
    .split('/')
    .map((part) => part.replace(/~1/g, '/').replace(/~0/g, '~'));
}

export function validateDisclosurePolicy(policy: DisclosurePolicy = {}): void {
  if (policy.payloadAccess && !['deny', 'allow'].includes(policy.payloadAccess)) {
    throw new Error('payloadAccess must be deny or allow');
  }
  const paths = [...(policy.allowedPaths ?? []), ...(policy.redactPaths ?? [])];
  if (paths.length > 100 || paths.some((path) => path.length > 256)) {
    throw new Error('Too many or overly long redaction paths');
  }
  paths.forEach(parsePointer);
  if (
    policy.maxValueBytes !== undefined &&
    (!Number.isInteger(policy.maxValueBytes) ||
      policy.maxValueBytes < 1 ||
      policy.maxValueBytes > 1_048_576)
  ) {
    throw new Error('maxValueBytes must be between 1 and 1048576');
  }
}

function matches(pattern: string[], path: string[]): boolean {
  return (
    pattern.length === path.length &&
    pattern.every((part, index) => part === '*' || part === path[index])
  );
}

function isPrefix(pattern: string[], path: string[]): boolean {
  return (
    pattern.length >= path.length &&
    path.every((part, index) => pattern[index] === '*' || pattern[index] === part)
  );
}

function select(value: unknown, path: string[], allowed: string[][]): unknown | typeof OMIT {
  if (allowed.some((pattern) => matches(pattern, path))) return value;
  if (!allowed.some((pattern) => isPrefix(pattern, path))) return OMIT;
  if (Array.isArray(value)) {
    return value.map((item, index) => {
      const selected = select(item, [...path, String(index)], allowed);
      return selected === OMIT ? '[HIDDEN]' : selected;
    });
  }
  if (value && typeof value === 'object') {
    const output: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      const selected = select(item, [...path, key], allowed);
      if (selected !== OMIT) output[key] = selected;
    }
    return output;
  }
  return OMIT;
}

function redact(value: unknown, path: string[], patterns: string[][]): unknown {
  if (patterns.some((pattern) => matches(pattern, path))) return '[REDACTED]';
  if (Array.isArray(value))
    return value.map((item, index) => redact(item, [...path, String(index)], patterns));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, redact(item, [...path, key], patterns)]),
    );
  }
  return value;
}

export function disclose(value: unknown, policy: DisclosurePolicy = {}): DisclosedValue {
  if (value === undefined) return { visibility: 'hidden', reason: 'unavailable' };
  const allowed = (policy.allowedPaths ?? []).map(parsePointer);
  const patterns = (policy.redactPaths ?? []).map(parsePointer);
  if (policy.payloadAccess !== 'allow' && allowed.length === 0) {
    return { visibility: 'hidden', reason: 'policy' };
  }
  try {
    const selected = allowed.length ? select(value, [], allowed) : value;
    if (selected === OMIT) return { visibility: 'hidden', reason: 'policy' };
    const safe = redact(selected, [], patterns);
    const serialized = JSON.stringify(safe);
    if (serialized === undefined) return { visibility: 'hidden', reason: 'unavailable' };
    if (Buffer.byteLength(serialized) > (policy.maxValueBytes ?? 65_536)) {
      return { visibility: 'hidden', reason: 'size' };
    }
    return { visibility: allowed.length ? 'partial' : 'visible', value: JSON.parse(serialized) };
  } catch {
    return { visibility: 'hidden', reason: 'unavailable' };
  }
}

export function sanitizeFailureText(value: string): string {
  return value
    .slice(0, MAX_TEXT_LENGTH)
    .replace(/\bBearer\s+\S+/gi, 'Bearer [REDACTED]')
    .replace(/\b(redis(?:s)?):\/\/[^\s]+/gi, '$1://[REDACTED]')
    .replace(
      /\b(authorization|password|secret|token|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi,
      '$1=[REDACTED]',
    );
}

export function safeJobSummary(job: RawJob): JobMetadata {
  return {
    id: job.id,
    name: job.name,
    state: job.state,
    attemptsMade: job.attemptsMade,
    timestamp: job.timestamp,
    processedOn: job.processedOn,
    finishedOn: job.finishedOn,
    delay: job.delay,
    priority: job.priority,
    progress: typeof job.progress === 'number' ? job.progress : '[HIDDEN]',
    repeatJobKey: job.repeatJobKey,
  };
}

export function safeJobDetail(job: RawJob, policy: DisclosurePolicy = {}): SafeJobDetail {
  return {
    ...safeJobSummary(job),
    payload: disclose(job.data, policy),
    returnValue: disclose(job.returnValue, policy),
    failureReason: policy.showFailureDetails
      ? sanitizeFailureText(job.failedReason ?? '')
      : job.failedReason
        ? '[HIDDEN]'
        : '',
    stacktrace: policy.showFailureDetails
      ? job.stacktrace.slice(0, 10).map(sanitizeFailureText)
      : [],
  };
}
