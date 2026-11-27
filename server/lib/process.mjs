import { aggregate, ValidationError } from './csv.mjs';
import { bucket } from './clients.mjs';
import { loadJob } from './jobs.mjs';

export async function processChunk(
  { db, objects, redis },
  id,
  { heartbeat, signal } = {},
) {
  const { rows } = await db.query(
    `SELECT c.*,j.status FROM chunks c JOIN jobs j ON j.id=c.job_id WHERE c.id=$1`,
    [id],
  );
  const chunk = rows[0];
  if (!chunk || chunk.completed || chunk.status === 'failed') return;
  const input = await objects.getObject(bucket, chunk.object_key);
  const totals = await aggregate(input, { heartbeat, signal });
  const tx = await db.connect();
  try {
    await tx.query('BEGIN');
    const {
      rows: [job],
    } = await tx.query('SELECT status FROM jobs WHERE id=$1 FOR UPDATE', [
      chunk.job_id,
    ]);
    if (job.status === 'failed') {
      await tx.query('ROLLBACK');
      return;
    }
    const claimed = await tx.query(
      'UPDATE chunks SET completed=true WHERE id=$1 AND NOT completed',
      [id],
    );
    if (!claimed.rowCount) {
      await tx.query('ROLLBACK');
      return;
    }
    for (const region of [...totals.keys()].sort()) {
      await tx.query(
        `INSERT INTO job_sales(job_id,region,total_cents) VALUES($1,$2,$3)
        ON CONFLICT(job_id,region) DO UPDATE SET total_cents=job_sales.total_cents+excluded.total_cents`,
        [chunk.job_id, region, totals.get(region).toString()],
      );
    }
    await tx.query(
      `UPDATE jobs SET completed_chunks=completed_chunks+1,
      status=CASE WHEN completed_chunks+1=total_chunks THEN 'completed' ELSE 'processing' END WHERE id=$1`,
      [chunk.job_id],
    );
    await tx.query('COMMIT');
  } catch (error) {
    await tx.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    tx.release();
  }
  // Refresh from committed state, never increment Redis independently.
  await loadJob(db, redis, chunk.job_id, { cached: false }).catch(() => {});
}
export function terminalError(error) {
  return error instanceof ValidationError || error.code === '22003';
}
export async function markFailed({ db, redis }, id) {
  const { rows } = await db.query(
    `UPDATE jobs SET status='failed',error=$2
    WHERE id=(SELECT job_id FROM chunks WHERE id=$1) AND status NOT IN ('failed','completed') RETURNING id`,
    [id, 'Stored CSV is invalid or its totals exceed the supported range'],
  );
  if (rows[0])
    await loadJob(db, redis, rows[0].id, { cached: false }).catch(() => {});
}
export async function dispatchOnce(db, producer) {
  const { rows } =
    await db.query(`SELECT c.id FROM chunks c JOIN jobs j ON j.id=c.job_id
    WHERE NOT c.completed AND j.status IN ('queued','processing') AND
    (c.published_at IS NULL OR c.published_at < now()-interval '5 minutes') ORDER BY c.id LIMIT 100`);
  for (const { id } of rows) {
    await producer.send({
      topic: 'file-chunks',
      acks: -1,
      messages: [{ key: id, value: JSON.stringify({ chunk_id: id }) }],
    });
    await db.query('UPDATE chunks SET published_at=now() WHERE id=$1', [id]);
  }
}
