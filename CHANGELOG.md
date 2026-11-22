# Changelog

## Unreleased — Stage 5 release readiness

- Add an external tarball install and TypeScript consumer check to CI.
- Document the tested compatibility set and monitor upgrade and rollback steps.

## Stage 4 — audited management

- Add an explicitly enabled admin namespace with separate authorization, queue scope, CSRF protection for embedded sessions, and a read-only default.
- Add state-checked single actions, previewed bulk retry and clean, advisory cancellation flags, per-item results, and confirmation UI.
- Add append-only audit events, AOF-backed intent/result recording, idempotency, rate limits, and queue locks.

## Unreleased — Stage 3 history and alerts

- Add opt-in, bounded Redis aggregate history with explicit missing-period gaps and dashboard trends.
- Add server-configured sustained alerts, persistent incidents, retryable webhook and embedded email delivery, and alert history.
- Add authenticated, queue-scoped Prometheus metrics and history/alert API routes.

## Unreleased — Stage 2 diagnostics

- Add optional payload-free worker heartbeats with TTL-based online and stale labels.
- Add authorized SSE queue updates, queue comparison, sampled diagnostics, and in-memory backlog trend.
- Add reconstructed job timeline, bounded flow and scheduler metadata, duration fields, retry and error filters, and light theme.

## Unreleased — Stage 1 preview

- Add a read-only dashboard and job inspector.
- Add embedded and standalone monitor factories, queue permissions, job paging, and exact lookup.
- Add server-side payload disclosure controls, bounded reads, and stale snapshot handling.
- Prepare a typed local package tarball. Public publication remains pending npm scope verification and release checks.
- Add an explicit `success` flag to the API envelope; failures retain stable error codes and metadata.
