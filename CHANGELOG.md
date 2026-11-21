# Changelog

## Unreleased — Stage 2 diagnostics

- Add optional payload-free worker heartbeats with TTL-based online and stale labels.
- Add authorized SSE queue updates, queue comparison, sampled diagnostics, and in-memory backlog trend.
- Add reconstructed job timeline, bounded flow links, duration fields, retry and error filters, and light theme.

## Unreleased — Stage 1 preview

- Add a read-only dashboard and job inspector.
- Add embedded and standalone monitor factories, queue permissions, job paging, and exact lookup.
- Add server-side payload disclosure controls, bounded reads, and stale snapshot handling.
- Prepare a typed local package tarball. Public publication remains pending npm scope verification and release checks.
- Add an explicit `success` flag to the API envelope; failures retain stable error codes and metadata.
