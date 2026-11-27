import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createClients, bucket } from '../../server/lib/clients.mjs';
import { processChunk } from '../../server/lib/process.mjs';
import { loadJob, cacheJob } from '../../server/lib/jobs.mjs';

// Run only in the opt-in Compose test container. These use real PostgreSQL,
// Redis and MinIO; the separate smoke script exercises Kafka end to end.
test(
  'real services: concurrent duplicate processing, Redis monotonic cache and fallback',
  { timeout: 60000 },
  async () => {
    const clients = createClients(),
      id = randomUUID(),
      key = `${id}/0.csv`,
      chunk = `${id}-0`;
    try {
      if (!clients.redis.isReady)
        await once(clients.redis, 'ready', {
          signal: AbortSignal.timeout(10000),
        });
      const data = Buffer.from(
        '1,North,125.50,2026-01-01T00:00:00Z\n2,North,74.50,2026-01-01T00:00:00Z\n',
      );
      await clients.objects.putObject(bucket, key, data, data.length);
      await clients.db.query(
        'INSERT INTO jobs(id,filename,total_chunks,row_count) VALUES($1,$2,1,2)',
        [id, 'integration.csv'],
      );
      await clients.db.query(
        'INSERT INTO chunks(id,job_id,object_key) VALUES($1,$2,$3)',
        [chunk, id, key],
      );
      await Promise.all(
        Array.from({ length: 4 }, () => processChunk(clients, chunk)),
      );
      const {
        rows: [result],
      } = await clients.db.query(
        'SELECT total_cents FROM job_sales WHERE job_id=$1',
        [id],
      );
      assert.equal(result.total_cents, '20000');
      const completed = await loadJob(clients.db, clients.redis, id, {
        cached: false,
      });
      assert.equal(completed.completed_chunks, 1);
      assert.equal(completed.status, 'completed');
      await cacheJob(clients.redis, {
        ...completed,
        status: 'processing',
        completed_chunks: 0,
        progress: 0,
      });
      const cached = JSON.parse(await clients.redis.get(`flow:job:${id}`));
      assert.equal(cached.status, 'completed');
      assert.equal(cached.completed_chunks, 1);
      // Simulate Redis being inaccessible to the status reader; PostgreSQL still serves results.
      const unavailable = {
        isReady: true,
        get: async () => {
          throw Error('offline');
        },
        eval: async () => {
          throw Error('offline');
        },
      };
      assert.equal(
        (await loadJob(clients.db, unavailable, id)).status,
        'completed',
      );
    } finally {
      await clients.db.query('DELETE FROM job_sales WHERE job_id=$1', [id]);
      await clients.db.query('DELETE FROM chunks WHERE job_id=$1', [id]);
      await clients.db.query('DELETE FROM jobs WHERE id=$1', [id]);
      await clients.objects.removeObject(bucket, key).catch(() => {});
      if (clients.redis.isReady) await clients.redis.del(`flow:job:${id}`);
      clients.redis.destroy();
      clients.objects.close();
      await clients.db.end();
    }
  },
);
