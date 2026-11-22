import type { HistorySample } from './history.js';

export type AlertMetric =
  | 'failed_count'
  | 'failure_rate'
  | 'backlog'
  | 'waiting_age'
  | 'heartbeat_gap';
export type AlertChannel = 'webhook' | 'email';

export interface AlertRule {
  id: string;
  queue: string;
  metric: AlertMetric;
  threshold: number;
  sustainMs: number;
  cooldownMs: number;
  channel: AlertChannel;
  workerId?: string;
  enabled: boolean;
}

export interface AlertIncident {
  id: string;
  ruleId: string;
  queue: string;
  metric: AlertMetric;
  openedAt: number;
  resolvedAt?: number;
  status: 'open' | 'resolved';
  lastValue: number;
}

export interface RuleState {
  pendingSince?: number;
  incidentId?: string;
  cooldownUntil?: number;
  lastEvaluatedAt?: number;
}

export interface NotificationEvent {
  id: string;
  incidentId: string;
  ruleId: string;
  queue: string;
  metric: AlertMetric;
  threshold: number;
  value: number;
  kind: 'opening' | 'recovery';
  channel: AlertChannel;
  at: number;
  attempts: number;
  nextRetryAt: number;
  status: 'pending' | 'sent' | 'failed';
  lastErrorCode?: string;
}

export interface AlertStorage {
  lease(key: string, ttlMs: number): Promise<boolean>;
  loadRuleState(ruleId: string): Promise<RuleState>;
  saveRuleState(ruleId: string, state: RuleState): Promise<void>;
  loadIncident(id: string): Promise<AlertIncident | null>;
  saveIncident(incident: AlertIncident): Promise<void>;
  commitIncident(
    ruleId: string,
    state: RuleState,
    incident: AlertIncident,
    event: NotificationEvent,
  ): Promise<void>;
  listIncidents(limit: number): Promise<AlertIncident[]>;
  enqueueNotification(event: NotificationEvent): Promise<void>;
  dueNotifications(now: number, limit: number): Promise<NotificationEvent[]>;
  listNotifications(limit: number): Promise<NotificationEvent[]>;
  saveNotification(event: NotificationEvent): Promise<void>;
  finishNotification(event: NotificationEvent): Promise<void>;
  claimDelivery(id: string, ttlMs: number): Promise<boolean>;
  rememberWorker(queue: string, id: string, lastHeartbeat: number): Promise<void>;
  lastWorkerHeartbeat(queue: string, id: string): Promise<number | null>;
}

export function validateAlertRules(rules: AlertRule[], queues: string[]): void {
  if (!Array.isArray(rules) || rules.length > 100)
    throw new Error('Alert rules must be an array of at most 100 rules');
  const ids = new Set<string>();
  for (const rule of rules) {
    if (!rule || typeof rule !== 'object') throw new Error('Invalid alert rule');
    if (!/^[A-Za-z0-9._-]{1,80}$/.test(rule.id) || ids.has(rule.id))
      throw new Error('Alert rule IDs must be unique and URL-safe');
    ids.add(rule.id);
    if (!queues.includes(rule.queue)) throw new Error('Alert rule refers to an unconfigured queue');
    if (
      !['failed_count', 'failure_rate', 'backlog', 'waiting_age', 'heartbeat_gap'].includes(
        rule.metric,
      )
    )
      throw new Error('Invalid alert metric');
    if (!['webhook', 'email'].includes(rule.channel)) throw new Error('Invalid alert channel');
    if (typeof rule.enabled !== 'boolean') throw new Error('Alert rule enabled must be boolean');
    if (!Number.isFinite(rule.threshold) || rule.threshold <= 0)
      throw new Error('Alert threshold must be positive');
    if (
      !Number.isInteger(rule.sustainMs) ||
      rule.sustainMs < 0 ||
      rule.sustainMs > 24 * 60 * 60_000
    )
      throw new Error('Invalid sustained window');
    if (
      !Number.isInteger(rule.cooldownMs) ||
      rule.cooldownMs < 0 ||
      rule.cooldownMs > 7 * 24 * 60 * 60_000
    )
      throw new Error('Invalid alert cooldown');
    if (
      rule.metric === 'heartbeat_gap' &&
      (!rule.workerId || !/^[A-Za-z0-9._-]{1,128}$/.test(rule.workerId))
    )
      throw new Error('Heartbeat gap rule requires an instrumented worker ID');
  }
}

export function alertMetricValue(
  rule: AlertRule,
  sample: HistorySample,
  workerHeartbeatAt: number | null,
): number | null {
  switch (rule.metric) {
    case 'failed_count':
      return sample.counts.failed;
    case 'failure_rate':
      return sample.failedPerMinute;
    case 'backlog':
      return sample.counts.waiting + sample.counts.delayed + sample.counts['waiting-children'];
    case 'waiting_age':
      return sample.oldestWaitingAgeMs;
    case 'heartbeat_gap':
      return workerHeartbeatAt === null ? null : Math.max(0, sample.observedAt - workerHeartbeatAt);
  }
}

export class AlertEngine {
  constructor(
    private readonly store: AlertStorage,
    readonly rules: AlertRule[],
    private readonly evaluationIntervalMs: number,
  ) {}

  async evaluate(
    sample: HistorySample,
    workerHeartbeats: Map<string, number> = new Map(),
  ): Promise<void> {
    for (const rule of this.rules) {
      if (!rule.enabled || rule.queue !== sample.queue) continue;
      const leaseKey = `alert:${rule.id}:${Math.floor(sample.observedAt / this.evaluationIntervalMs)}`;
      if (!(await this.store.lease(leaseKey, this.evaluationIntervalMs * 2))) continue;
      let heartbeatAt: number | null = null;
      if (rule.metric === 'heartbeat_gap' && rule.workerId) {
        const observed = workerHeartbeats.get(rule.workerId);
        if (observed !== undefined) {
          heartbeatAt = observed;
          await this.store.rememberWorker(rule.queue, rule.workerId, observed);
        } else {
          heartbeatAt = await this.store.lastWorkerHeartbeat(rule.queue, rule.workerId);
        }
      }
      const value = alertMetricValue(rule, sample, heartbeatAt);
      if (value === null) continue;
      const state = await this.store.loadRuleState(rule.id);
      if (
        state.lastEvaluatedAt &&
        sample.observedAt - state.lastEvaluatedAt > this.evaluationIntervalMs * 1.5
      )
        state.pendingSince = undefined;
      state.lastEvaluatedAt = sample.observedAt;
      const violated = value >= rule.threshold;
      if (violated) {
        if (state.incidentId) {
          const incident = await this.store.loadIncident(state.incidentId);
          if (incident?.status === 'open') {
            await this.store.saveIncident({ ...incident, lastValue: value });
            await this.store.saveRuleState(rule.id, state);
            continue;
          }
        }
        state.pendingSince ??= sample.observedAt;
        if (
          sample.observedAt - state.pendingSince < rule.sustainMs ||
          sample.observedAt < (state.cooldownUntil ?? 0)
        ) {
          await this.store.saveRuleState(rule.id, state);
          continue;
        }
        const incident: AlertIncident = {
          id: `${rule.id}:${sample.observedAt}`,
          ruleId: rule.id,
          queue: rule.queue,
          metric: rule.metric,
          openedAt: sample.observedAt,
          status: 'open',
          lastValue: value,
        };
        state.incidentId = incident.id;
        state.pendingSince = undefined;
        await this.store.commitIncident(
          rule.id,
          state,
          incident,
          this.event(rule, incident, 'opening', value, sample.observedAt),
        );
      } else {
        state.pendingSince = undefined;
        if (state.incidentId) {
          const incident = await this.store.loadIncident(state.incidentId);
          if (incident?.status === 'open') {
            const resolved = {
              ...incident,
              status: 'resolved' as const,
              resolvedAt: sample.observedAt,
              lastValue: value,
            };
            state.cooldownUntil = sample.observedAt + rule.cooldownMs;
            state.incidentId = undefined;
            await this.store.commitIncident(
              rule.id,
              state,
              resolved,
              this.event(rule, resolved, 'recovery', value, sample.observedAt),
            );
            continue;
          }
          state.incidentId = undefined;
        }
        await this.store.saveRuleState(rule.id, state);
      }
    }
  }

  private event(
    rule: AlertRule,
    incident: AlertIncident,
    kind: 'opening' | 'recovery',
    value: number,
    at: number,
  ): NotificationEvent {
    return {
      id: `${incident.id}:${kind}`,
      incidentId: incident.id,
      ruleId: rule.id,
      queue: rule.queue,
      metric: rule.metric,
      threshold: rule.threshold,
      value,
      kind,
      channel: rule.channel,
      at,
      attempts: 0,
      nextRetryAt: at,
      status: 'pending',
    };
  }
}
