export function jobJSON(row) {
  return {
    job_id: row.id,
    filename: row.filename,
    status: row.status,
    total_chunks: row.total_chunks,
    completed_chunks: row.completed_chunks,
    rows: Number(row.row_count),
    progress: row.total_chunks
      ? Math.floor((row.completed_chunks * 100) / row.total_chunks)
      : 0,
    error: row.error || '',
    created_at: new Date(row.created_at).toISOString(),
  };
}
const updateScript = `
local old=redis.call('GET',KEYS[1])
local incoming=cjson.decode(ARGV[1])
if old then
 local previous=cjson.decode(old)
 if previous.completed_chunks > incoming.completed_chunks then return 0 end
 if (previous.status == 'completed' or previous.status == 'failed') and incoming.status ~= previous.status then return 0 end
end
redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2])
return 1`;
// Cache is disposable and short-lived. Monotonic writes prevent delayed updates
// from reducing progress; expiration bounds stale state after a worker crash.
function bounded(redis) {
  return redis.withAbortSignal
    ? redis.withAbortSignal(AbortSignal.timeout(500))
    : redis;
}
export async function cacheJob(redis, job) {
  if (!redis?.isReady) return;
  try {
    await bounded(redis).eval(updateScript, {
      keys: [`flow:job:${job.job_id}`],
      arguments: [JSON.stringify(job), '10'],
    });
  } catch {
    /* Cache loss must never fail durable work. */
  }
}
export async function loadJob(db, redis, id, { cached = true } = {}) {
  if (cached && redis?.isReady) {
    try {
      const value = await bounded(redis).get(`flow:job:${id}`);
      if (value) {
        const parsed = JSON.parse(value);
        if (
          parsed.job_id === id &&
          Number.isInteger(parsed.completed_chunks) &&
          ['queued', 'processing', 'completed', 'failed'].includes(
            parsed.status,
          )
        )
          return parsed;
      }
    } catch {}
  }
  const { rows } = await db.query('SELECT * FROM jobs WHERE id=$1', [id]);
  if (!rows.length) return null;
  const job = jobJSON(rows[0]);
  await cacheJob(redis, job);
  return job;
}
