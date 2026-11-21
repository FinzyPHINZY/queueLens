FROM node:22-bookworm-slim
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/core/package.json ./packages/core/package.json
COPY packages/bullmq-adapter/package.json ./packages/bullmq-adapter/package.json
COPY packages/server/package.json ./packages/server/package.json
COPY apps/standalone/package.json ./apps/standalone/package.json
COPY apps/example-bullmq/package.json ./apps/example-bullmq/package.json
RUN pnpm install --frozen-lockfile
COPY tsconfig.json tsconfig.build.json eslint.config.mjs ./
COPY packages ./packages
COPY apps ./apps
RUN pnpm build
CMD ["pnpm", "start"]
