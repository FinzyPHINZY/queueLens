import { createHash, randomBytes, randomUUID } from 'node:crypto';
import type { Job, Queue } from 'bullmq';
import type { MonitorErrorCode } from '../../core/src/monitor.js';
import {
  validActorId,
  validOperationKey,
  type AuditEvent,
  type ManagementAction,
  type ManagementResult,
  type ManagementStore,
} from '../../core/src/management.js';

export class ManagementError extends Error {
  constructor(
    readonly code: MonitorErrorCode,
    readonly status: number,
    readonly operationId?: string,
  ) {
    super(code);
  }
}

export interface ManagementCommand {
  jobId?: string;
  jobIds?: string[];
  state?: 'completed' | 'failed';
  olderThanMs?: number;
  limit?: number;
  previewToken?: string;
}

export interface ManagementPreview {
  action: 'bulk-retry' | 'clean';
  queue: string;
  jobIds: string[];
  eligible: { id: string; eligible: boolean; code?: string }[];
  previewToken: string;
  expiresAt: string;
}

const validJobId = (id: unknown): id is string =>
  typeof id === 'string' &&
  id.length >= 1 &&
  id.length <= 128 &&
  !id.includes('/') &&
  [...id].every((character) => character.charCodeAt(0) >= 32);
const safeCode = (cause: unknown) => {
  if (cause instanceof ManagementError) return cause.code;
  return 'BULLMQ_OPERATION_FAILED';
};

export class ManagementService {
  private readonly queues: Map<string, Queue>;

  constructor(
    queues: Queue[],
    private readonly store: ManagementStore,
  ) {
    this.queues = new Map(queues.map((queue) => [queue.name, queue]));
  }

  private queue(name: string): Queue {
    const queue = this.queues.get(name);
    if (!queue) throw new ManagementError('QUEUE_NOT_FOUND', 404);
    return queue;
  }

  private normalize(action: ManagementAction, input: ManagementCommand): ManagementCommand {
    if (action === 'pause' || action === 'resume') return {};
    if (action === 'bulk-retry' || action === 'clean') {
      if (
        !Array.isArray(input.jobIds) ||
        input.jobIds.length < 1 ||
        input.jobIds.length > 25 ||
        input.jobIds.some((id) => !validJobId(id)) ||
        new Set(input.jobIds).size !== input.jobIds.length
      )
        throw new ManagementError('INVALID_TARGETS', 400);
      if (
        action === 'clean' &&
        ((input.state !== 'completed' && input.state !== 'failed') ||
          !Number.isSafeInteger(input.olderThanMs) ||
          (input.olderThanMs ?? 0) < 60_000 ||
          (input.olderThanMs ?? 0) > 30 * 24 * 60 * 60_000)
      )
        throw new ManagementError('INVALID_CLEAN_FILTER', 400);
      if (!input.previewToken || input.previewToken.length > 256)
        throw new ManagementError('PREVIEW_REQUIRED', 400);
      return {
        jobIds: input.jobIds,
        state: action === 'clean' ? input.state : undefined,
        olderThanMs: action === 'clean' ? input.olderThanMs : undefined,
        previewToken: input.previewToken,
      };
    }
    if (!validJobId(input.jobId)) throw new ManagementError('INVALID_JOB_ID', 400);
    return { jobId: input.jobId };
  }

  private async verifyPreview(
    actor: string,
    queue: string,
    action: string,
    command: ManagementCommand,
  ): Promise<void> {
    if (!command.previewToken || !/^[a-f0-9]{48}$/.test(command.previewToken))
      throw new ManagementError('PREVIEW_EXPIRED', 409);
    const record = await this.store.loadPreview(command.previewToken);
    if (!record || record.expiresAt < Date.now()) throw new ManagementError('PREVIEW_EXPIRED', 409);
    if (
      record.actorId !== actor ||
      record.queue !== queue ||
      record.action !== action ||
      JSON.stringify(record.jobIds) !== JSON.stringify(command.jobIds) ||
      record.state !== command.state ||
      record.olderThanMs !== command.olderThanMs
    )
      throw new ManagementError('PREVIEW_MISMATCH', 409);
  }

  async preview(
    actorId: string,
    queueName: string,
    action: 'bulk-retry' | 'clean',
    input: ManagementCommand,
  ): Promise<ManagementPreview> {
    if (!validActorId(actorId)) throw new ManagementError('INVALID_ACTOR', 403);
    const queue = this.queue(queueName);
    if (!(await this.store.countAttempt(`${actorId}:${queueName}:preview`, 60)))
      throw new ManagementError('RATE_LIMITED', 429);
    let ids: string[];
    if (action === 'bulk-retry') {
      if (
        !Array.isArray(input.jobIds) ||
        input.jobIds.length < 1 ||
        input.jobIds.length > 25 ||
        input.jobIds.some((id) => !validJobId(id)) ||
        new Set(input.jobIds).size !== input.jobIds.length
      )
        throw new ManagementError('INVALID_TARGETS', 400);
      ids = input.jobIds;
    } else {
      if (
        (input.state !== 'completed' && input.state !== 'failed') ||
        !Number.isSafeInteger(input.olderThanMs) ||
        (input.olderThanMs ?? 0) < 60_000 ||
        (input.olderThanMs ?? 0) > 30 * 24 * 60 * 60_000 ||
        !Number.isInteger(input.limit) ||
        (input.limit ?? 0) < 1 ||
        (input.limit ?? 0) > 25
      )
        throw new ManagementError('INVALID_CLEAN_FILTER', 400);
      const jobs = await queue.getJobs([input.state], 0, input.limit! - 1, true);
      ids = jobs
        .filter(
          (job) => (job.finishedOn ?? Number.POSITIVE_INFINITY) <= Date.now() - input.olderThanMs!,
        )
        .map((job) => job.id!);
    }
    const eligible = await Promise.all(
      ids.map(async (id) => {
        const job = await queue.getJob(id);
        if (!job) return { id, eligible: false, code: 'JOB_NOT_FOUND' };
        const state = await job.getState();
        if (action === 'bulk-retry')
          return {
            id,
            eligible: state === 'failed',
            code: state === 'failed' ? undefined : 'STATE_CHANGED',
          };
        const safe =
          (await this.removable(job, state)) &&
          state === input.state &&
          (job.finishedOn ?? Number.POSITIVE_INFINITY) <= Date.now() - input.olderThanMs!;
        return { id, eligible: safe, code: safe ? undefined : 'NOT_ELIGIBLE' };
      }),
    );
    if (!ids.length) throw new ManagementError('NO_ELIGIBLE_JOBS', 409);
    const expiresAt = Date.now() + 300_000;
    const previewToken = randomBytes(24).toString('hex');
    await this.store.savePreview(previewToken, {
      actorId,
      queue: queueName,
      action,
      jobIds: ids,
      state: input.state,
      olderThanMs: input.olderThanMs,
      expiresAt,
    });
    return {
      action,
      queue: queueName,
      jobIds: ids,
      eligible,
      previewToken,
      expiresAt: new Date(expiresAt).toISOString(),
    };
  }

  private async removable(job: Job, state: string): Promise<boolean> {
    if (
      !['waiting', 'delayed', 'completed', 'failed'].includes(state) ||
      job.parent ||
      job.opts.repeat ||
      job.repeatJobKey
    )
      return false;
    const dependencies = await job.getDependenciesCount();
    return Object.values(dependencies).every((count) => !count);
  }

  private async single(
    queue: Queue,
    action: ManagementAction,
    command: ManagementCommand,
  ): Promise<void> {
    if (action === 'pause' || action === 'resume') {
      const paused = await queue.isPaused();
      if ((action === 'pause' && paused) || (action === 'resume' && !paused))
        throw new ManagementError('STATE_CHANGED', 409);
      if (action === 'pause') await queue.pause();
      else await queue.resume();
      return;
    }
    const job = await queue.getJob(command.jobId!);
    if (!job) throw new ManagementError('JOB_NOT_FOUND', 404);
    const state = await job.getState();
    if (action === 'retry') {
      if (state !== 'failed') throw new ManagementError('STATE_CHANGED', 409);
      await job.retry('failed');
    } else if (action === 'promote') {
      if (state !== 'delayed') throw new ManagementError('STATE_CHANGED', 409);
      await job.promote();
    } else if (action === 'remove') {
      if (!(await this.removable(job, state))) throw new ManagementError('NOT_ELIGIBLE', 409);
      await job.remove({ removeChildren: false });
    } else if (action === 'cancel-request') {
      if (state !== 'active') throw new ManagementError('STATE_CHANGED', 409);
      await this.store.requestCancellation(queue.name, job.id!, 3_600_000);
    }
  }

  async execute(
    actorId: string,
    action: ManagementAction,
    queueName: string,
    input: ManagementCommand,
    idempotencyKey: string,
    requestId = randomUUID(),
  ): Promise<ManagementResult> {
    if (!validActorId(actorId)) throw new ManagementError('INVALID_ACTOR', 403);
    if (!validOperationKey(idempotencyKey))
      throw new ManagementError('IDEMPOTENCY_KEY_REQUIRED', 400);
    const queue = this.queue(queueName);
    const command = this.normalize(action, input);
    const fingerprint = createHash('sha256')
      .update(JSON.stringify({ actorId, action, queueName, command }))
      .digest('hex');
    const replay = await this.store.getOperation(idempotencyKey);
    if (replay) {
      if (replay.fingerprint !== fingerprint)
        throw new ManagementError('IDEMPOTENCY_CONFLICT', 409);
      if (replay.status === 'pending') throw new ManagementError('OPERATION_PENDING', 409);
      if (replay.result) return replay.result;
      throw new ManagementError(replay.errorCode ?? 'OPERATION_REJECTED', 409);
    }
    if (action === 'bulk-retry' || action === 'clean')
      await this.verifyPreview(actorId, queueName, action, command);
    const operationId = randomUUID();
    const base: AuditEvent = {
      operationId,
      requestId,
      actorId,
      action,
      queue: queueName,
      targetId: command.jobId,
      phase: 'intent',
      at: new Date().toISOString(),
    };
    const reservation = await this.store.reserve(idempotencyKey, fingerprint, base);
    if (reservation !== 'new') {
      if (reservation.fingerprint !== fingerprint)
        throw new ManagementError('IDEMPOTENCY_CONFLICT', 409);
      if (reservation.status === 'pending') throw new ManagementError('OPERATION_PENDING', 409);
      if (reservation.result) return reservation.result;
      throw new ManagementError(reservation.errorCode ?? 'OPERATION_REJECTED', 409);
    }
    let acquired = false;
    let applied = false;
    let mutationStarted = false;
    try {
      if (!(await this.store.countAttempt(`${actorId}:${queueName}`, 20)))
        throw new ManagementError('RATE_LIMITED', 429);
      acquired = await this.store.acquireQueue(queueName, operationId, 600_000);
      if (!acquired) throw new ManagementError('QUEUE_BUSY', 429);
      let result: ManagementResult;
      if (action === 'bulk-retry' || action === 'clean') {
        const items: NonNullable<ManagementResult['items']> = [];
        for (const id of command.jobIds!) {
          try {
            if (!(await this.store.renewQueue(queueName, operationId, 600_000)))
              throw new ManagementError('QUEUE_BUSY', 429);
            if (action === 'bulk-retry') await this.single(queue, 'retry', { jobId: id });
            else {
              const job = await queue.getJob(id);
              if (
                !job ||
                (await job.getState()) !== command.state ||
                (job.finishedOn ?? Number.POSITIVE_INFINITY) > Date.now() - command.olderThanMs! ||
                !(await this.removable(job, command.state!))
              )
                throw new ManagementError('NOT_ELIGIBLE', 409);
              await job.remove({ removeChildren: false });
            }
            items.push({ id, status: 'succeeded' });
          } catch (cause) {
            items.push(
              cause instanceof ManagementError
                ? { id, status: 'rejected', code: cause.code }
                : { id, status: 'uncertain', code: 'OUTCOME_UNCERTAIN' },
            );
          }
        }
        result = {
          operationId,
          action,
          queue: queueName,
          status: items.every((item) => item.status === 'succeeded') ? 'succeeded' : 'partial',
          items,
        };
      } else {
        mutationStarted = true;
        await this.single(queue, action, command);
        result = { operationId, action, queue: queueName, status: 'succeeded' };
      }
      applied = true;
      await this.store.finish(
        idempotencyKey,
        { fingerprint, status: 'complete', result },
        { ...base, phase: 'result', outcome: result.status, at: new Date().toISOString() },
      );
      return result;
    } catch (cause) {
      if (applied) throw new ManagementError('MANAGEMENT_OUTCOME_UNCERTAIN', 503, operationId);
      const uncertain = mutationStarted && !(cause instanceof ManagementError);
      const code = uncertain ? 'MANAGEMENT_OUTCOME_UNCERTAIN' : safeCode(cause);
      try {
        await this.store.finish(
          idempotencyKey,
          { fingerprint, status: 'complete', errorCode: code },
          {
            ...base,
            phase: 'result',
            outcome: uncertain ? 'uncertain' : 'rejected',
            code,
            at: new Date().toISOString(),
          },
        );
      } catch {
        throw new ManagementError('MANAGEMENT_OUTCOME_UNCERTAIN', 503, operationId);
      }
      if (cause instanceof ManagementError) throw cause;
      throw new ManagementError(code, 503, operationId);
    } finally {
      if (acquired) await this.store.releaseQueue(queueName, operationId).catch(() => undefined);
    }
  }

  async recentAudit(limit = 50): Promise<AuditEvent[]> {
    return this.store.recent(limit);
  }
  async cancellationRequested(queueName: string, jobId: string): Promise<boolean> {
    return this.store.cancellationRequested(queueName, jobId);
  }
  async close(): Promise<void> {
    await this.store.close();
  }
}
