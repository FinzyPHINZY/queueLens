import type { AlertChannel, AlertStorage, NotificationEvent } from '../../core/src/alerts.js';

export interface NotificationSink {
  readonly channel: AlertChannel;
  send(event: NotificationEvent): Promise<void>;
}

export interface EmailMessage {
  to: string;
  subject: string;
  text: string;
}

/** Embedded applications supply an SMTP or email-provider transport. */
export function createEmailSink(
  to: string,
  sendMail: (message: EmailMessage) => Promise<void>,
): NotificationSink {
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(to) || to.length > 254)
    throw new Error('Invalid alert email recipient');
  return {
    channel: 'email',
    async send(event) {
      const state = event.kind === 'opening' ? 'opened' : 'recovered';
      await sendMail({
        to,
        subject: `QueueLens alert ${state}: ${event.ruleId}`,
        text: `Event ${event.id}\nQueue: ${event.queue}\nMetric: ${event.metric}\nValue: ${event.value}\nThreshold: ${event.threshold}\nTime: ${new Date(event.at).toISOString()}\n`,
      });
    },
  };
}

export function createWebhookSink(rawUrl: string): NotificationSink {
  const url = new URL(rawUrl);
  if (
    url.username ||
    url.password ||
    (url.protocol !== 'https:' &&
      !(url.protocol === 'http:' && ['localhost', '127.0.0.1', '::1'].includes(url.hostname)))
  )
    throw new Error('Webhook URL must use HTTPS or local HTTP without embedded credentials');
  return {
    channel: 'webhook',
    async send(event) {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-queuelens-event-id': event.id },
        body: JSON.stringify({
          id: event.id,
          kind: event.kind,
          incidentId: event.incidentId,
          ruleId: event.ruleId,
          queue: event.queue,
          metric: event.metric,
          threshold: event.threshold,
          value: event.value,
          at: new Date(event.at).toISOString(),
        }),
        signal: AbortSignal.timeout(5_000),
      });
      if (!response.ok) throw new Error(`HTTP_${response.status}`);
    },
  };
}

async function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error('DELIVERY_TIMEOUT')), timeoutMs);
    timer.unref();
  });
  try {
    return await Promise.race([operation, deadline]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export class NotificationDispatcher {
  private timer: NodeJS.Timeout | undefined;
  private stopped = false;
  private running = false;

  constructor(
    private readonly store: AlertStorage,
    private readonly sinks: NotificationSink[],
  ) {}

  start(): void {
    if (this.timer || this.stopped) return;
    this.timer = setInterval(() => void this.dispatch().catch(() => undefined), 5_000);
    this.timer.unref();
    void this.dispatch().catch(() => undefined);
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  async dispatch(now = Date.now()): Promise<void> {
    if (this.running || this.stopped) return;
    this.running = true;
    try {
      const events = await this.store.dueNotifications(now, 20);
      for (const event of events) {
        if (this.stopped || event.status !== 'pending') continue;
        if (!(await this.store.claimDelivery(event.id, 10_000))) continue;
        const sink = this.sinks.find((item) => item.channel === event.channel);
        try {
          if (!sink) throw new Error('DESTINATION_UNAVAILABLE');
          await withTimeout(sink.send(event), 5_000);
          await this.store.finishNotification({
            ...event,
            status: 'sent',
            attempts: event.attempts + 1,
            lastErrorCode: undefined,
          });
        } catch (cause) {
          const attempts = event.attempts + 1;
          const code =
            cause instanceof Error &&
            /^HTTP_\d{3}$|^DESTINATION_UNAVAILABLE$|^DELIVERY_TIMEOUT$/.test(cause.message)
              ? cause.message
              : 'DELIVERY_FAILED';
          const updated: NotificationEvent = {
            ...event,
            attempts,
            lastErrorCode: code,
            status: attempts >= 5 ? 'failed' : 'pending',
            nextRetryAt: now + Math.min(60_000, Math.max(10_000, 2 ** attempts * 1_000)),
          };
          if (updated.status === 'failed') await this.store.finishNotification(updated);
          else await this.store.saveNotification(updated);
        }
      }
    } finally {
      this.running = false;
    }
  }
}
