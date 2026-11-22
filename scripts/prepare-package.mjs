import { cp, mkdir, rm } from 'node:fs/promises';

await rm('dist/dashboard', { recursive: true, force: true });
await cp('apps/dashboard/dist', 'dist/dashboard', { recursive: true });
await rm('packages/monitor/dist', { recursive: true, force: true });
await mkdir('packages/monitor/dist', { recursive: true });
await cp('dist/packages', 'packages/monitor/dist/packages', { recursive: true });
await cp('dist/dashboard', 'packages/monitor/dist/dashboard', { recursive: true });
await cp('README.md', 'packages/monitor/README.md');
await cp('LICENSE', 'packages/monitor/LICENSE');
