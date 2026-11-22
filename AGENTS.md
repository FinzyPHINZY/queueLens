# Repository Guidelines

## Project Structure & Module Organization

`packages/core/` holds read-only queue contracts, polling, history, and alert logic. `packages/bullmq-adapter/` wraps public BullMQ APIs, `packages/monitor/` assembles the service, and `packages/server/` handles HTTP. `apps/standalone/` runs the monitor, `apps/dashboard/` contains the React UI, and `apps/example-bullmq/` provides sample jobs. Tests live in `tests/unit/`, `tests/integration/`, and `tests/browser/`. Local plans in ignored `docs/` are not part of Git.

## Build, Test, and Development Commands

Run `pnpm install --frozen-lockfile`, then `pnpm build` for compiled output, `pnpm typecheck` for static checks, `pnpm lint` for ESLint, and `pnpm format:check` for Prettier. `pnpm dev` starts the standalone monitor and requires `MONITOR_TOKEN`. `pnpm example:worker` and `pnpm example:seed` exercise the sample queue. `pnpm test:unit` needs no Redis; `pnpm test` includes real Redis integration tests. See `README.md` for local Redis and Docker Compose setup.
Run `pnpm test:package` to install the built tarball in a separate temporary project and check its ESM and TypeScript exports; it downloads isolated dependencies.

## Coding Style & Naming Conventions

Use Node.js with strict TypeScript, two-space indentation, camelCase variables and functions, PascalCase types and components, and kebab-case directories. Run Prettier and ESLint before a pull request. Keep BullMQ calls behind the adapters and use public APIs. Keep mutations in the explicitly enabled, audited admin namespace; ordinary monitor routes remain read-only.

## Testing Guidelines

Tests use Node's test runner. Name files `*.test.ts` and state the behavior in each test name. Keep fixtures free of real credentials or customer payloads. Unit tests cover API behavior without sockets; integration tests require real Redis and BullMQ. Before release, verify authorization, redaction, pagination limits, outages, and no read-only mutations. Run `pnpm check` and the demo from a clean checkout.

## Commit & Pull Request Guidelines

Use short imperative commit subjects such as `Add queue state mapping`. Pull requests should describe observable behavior and security implications, list verification commands and results, and include screenshots for dashboard changes. If you have the local planning files, update `docs/BUILD_PLAN.md` and `docs/PRODUCT.md` when scope or requirement status changes; those files remain outside Git.
