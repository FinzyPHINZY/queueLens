import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import process from 'node:process';

const root = resolve(import.meta.dirname, '..');
const temporary = await mkdtemp(join(tmpdir(), 'queueharbor-package-'));
const tarball = join(temporary, 'monitor.tgz');
const run = (command, args, cwd = temporary) =>
  execFileSync(command, args, {
    cwd,
    stdio: 'inherit',
    timeout: 120_000,
    env: { ...process.env, npm_config_cache: join(temporary, 'npm-cache') },
  });

try {
  run('pnpm', ['--dir', join(root, 'packages/monitor'), 'pack', '--out', tarball], root);
  await writeFile(
    join(temporary, 'package.json'),
    JSON.stringify({ name: 'queueharbor-external-consumer', private: true, type: 'module' }),
  );
  run('npm', [
    'install',
    '--ignore-scripts',
    '--no-audit',
    '--no-fund',
    '--package-lock=false',
    tarball,
    'bullmq@5.81.5',
    'typescript@5.9.3',
    '@types/node@22.19.17',
  ]);
  const installed = join(temporary, 'node_modules/@queueharbor/monitor');
  const manifest = JSON.parse(await readFile(join(installed, 'package.json'), 'utf8'));
  assert.equal(manifest.name, '@queueharbor/monitor');
  assert.equal(manifest.version, '0.1.0');
  assert.equal(manifest.private, false);
  assert.equal(manifest.license, 'MIT');
  assert.match(await readFile(join(installed, 'README.md'), 'utf8'), /^# QueueHarbor/m);
  assert.match(await readFile(join(installed, 'LICENSE'), 'utf8'), /^MIT License/m);
  await writeFile(
    join(temporary, 'smoke.mjs'),
    `import assert from 'node:assert/strict';
import { createMonitor, createExpressMonitor, instrumentWorker, createCancellationProbe } from '@queueharbor/monitor';
assert.equal(typeof createMonitor, 'function');
assert.equal(typeof createExpressMonitor, 'function');
assert.equal(typeof instrumentWorker, 'function');
assert.equal(typeof createCancellationProbe, 'function');
`,
  );
  await writeFile(
    join(temporary, 'consumer.ts'),
    `import { createMonitor, type MonitorOptions, type ManagementOptions } from '@queueharbor/monitor';
import { Queue } from 'bullmq';
const options: MonitorOptions = { queues: [] as Queue[], token: 'example-token' };
const management: ManagementOptions | undefined = undefined;
void createMonitor; void options; void management;
`,
  );
  await writeFile(
    join(temporary, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        strict: true,
        noEmit: true,
        skipLibCheck: false,
      },
      files: ['consumer.ts'],
    }),
  );
  run(process.execPath, ['smoke.mjs']);
  run(process.execPath, [
    join(temporary, 'node_modules/typescript/bin/tsc'),
    '-p',
    'tsconfig.json',
  ]);
  process.stdout.write('External ESM package install, runtime import, and types passed.\n');
} finally {
  await rm(temporary, { recursive: true, force: true });
}
