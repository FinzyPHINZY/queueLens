# Repository Guidelines

## Project Structure & Module Organization

`packages/core/` holds read-only queue contracts and the registry, `packages/bullmq-adapter/` wraps public BullMQ APIs, and `packages/server/` handles HTTP. `apps/standalone/` runs the monitor; `apps/example-bullmq/` provides sample jobs. Tests live in `tests/unit/` and `tests/integration/`. Local planning and architecture notes live in ignored `docs/`; they are not part of the repository. The React dashboard and other later-stage components are still planned.

## Build, Test, and Development Commands

Run `pnpm install --frozen-lockfile`, then `pnpm build` for compiled output, `pnpm typecheck` for static checks, `pnpm lint` for ESLint, and `pnpm format:check` for Prettier. `pnpm dev` starts the standalone monitor and requires `MONITOR_TOKEN`. `pnpm example:worker` and `pnpm example:seed` exercise the sample queue. `pnpm test:unit` needs no Redis; `pnpm test` includes real Redis integration tests. See `README.md` for local Redis and Docker Compose setup.

## Coding Style & Naming Conventions

Use Node.js with strict TypeScript, two-space indentation, camelCase variables and functions, PascalCase types and components, and kebab-case directories. Run Prettier and ESLint before a pull request. Keep BullMQ calls behind the adapter and use public APIs. Preserve the read-only boundary; do not add queue mutations to monitor routes.

## Testing Guidelines

Tests use Node's test runner. Name files `*.test.ts` and state the behavior in each test name. Keep fixtures free of real credentials or customer payloads. Unit tests cover API behavior without sockets; integration tests require real Redis and BullMQ. Before release, verify authorization, redaction, pagination limits, outages, and no read-only mutations. Run `pnpm check` and the demo from a clean checkout.

## Commit & Pull Request Guidelines

Use short imperative commit subjects such as `Add queue state mapping`. Pull requests should describe observable behavior and security implications, list verification commands and results, and include screenshots for dashboard changes. If you have the local planning files, update `docs/BUILD_PLAN.md` and `docs/PRODUCT.md` when scope or requirement status changes; those files remain outside Git.
