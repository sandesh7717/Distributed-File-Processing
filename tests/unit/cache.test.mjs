import test from 'node:test';
import assert from 'node:assert/strict';
import { loadJob, cacheJob } from '../../server/lib/jobs.mjs';
const row = {
  id: 'id',
  filename: 'a.csv',
  status: 'processing',
  total_chunks: 2,
  completed_chunks: 1,
  row_count: '5',
  created_at: '2026-01-01T00:00:00Z',
};
test('status falls back to PostgreSQL on Redis failures', async () => {
  let queries = 0;
  const db = {
    query: async () => {
      queries++;
      return { rows: [row] };
    },
  };
  const redis = {
    isReady: true,
    get: async () => {
      throw Error('offline');
    },
    eval: async () => {
      throw Error('offline');
    },
  };
  assert.equal((await loadJob(db, redis, 'id')).progress, 50);
  assert.equal(queries, 1);
});
test('valid Redis hit avoids PostgreSQL and writes have short expiration', async () => {
  const redis = {
    isReady: true,
    get: async () =>
      JSON.stringify({
        job_id: 'id',
        completed_chunks: 1,
        status: 'processing',
      }),
    eval: async (script, options) => {
      assert.match(
        script,
        /previous.completed_chunks > incoming.completed_chunks/,
      );
      assert.match(script, /previous.status == 'failed'/);
      assert.equal(options.arguments[1], '10');
    },
  };
  assert.equal(
    (
      await loadJob(
        {
          query: () => {
            throw Error('must not query');
          },
        },
        redis,
        'id',
      )
    ).completed_chunks,
    1,
  );
  await cacheJob(redis, {
    job_id: 'id',
    completed_chunks: 1,
    status: 'processing',
  });
});
