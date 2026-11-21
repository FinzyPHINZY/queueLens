import { Queue } from 'bullmq';
import { redisOptions } from '../../../packages/bullmq-adapter/src/connection.js';

const queue = new Queue('demo', {
  connection: redisOptions(process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'),
});

try {
  const batch = Date.now().toString();
  await queue.add('complete', { sample: true }, { jobId: `complete-${batch}` });
  await queue.add('fail', { sample: true }, { jobId: `fail-${batch}` });
  await queue.add('slow', { sample: true }, { jobId: `slow-${batch}` });
  await queue.add('waiting', { sample: true }, { jobId: `waiting-${batch}` });
  await queue.add('delayed', { sample: true }, { jobId: `delayed-${batch}`, delay: 60_000 });
  console.log(`Seeded demo jobs with suffix ${batch}`);
} finally {
  await queue.close();
}
