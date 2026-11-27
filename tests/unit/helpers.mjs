import { Readable } from 'node:stream';
// A deliberately small test double, not a substitute for PostgreSQL integration.
export function fixtures() {
  const state = {
    jobs: new Map(),
    chunks: new Map(),
    sales: new Map(),
    objects: new Map(),
    fail: null,
    commits: 0,
    rollbacks: 0,
  };
  let snapshot;
  const db = {
    async connect() {
      return { ...db, release() {} };
    },
    async query(sql, args = []) {
      if (state.fail && sql.includes(state.fail))
        throw Error('injected database failure');
      if (sql === 'BEGIN') {
        snapshot = structuredClone({
          jobs: state.jobs,
          chunks: state.chunks,
          sales: state.sales,
        });
        return {};
      }
      if (sql === 'ROLLBACK') {
        if (snapshot) {
          Object.assign(state, snapshot);
          snapshot = null;
        }
        state.rollbacks++;
        return {};
      }
      if (sql === 'COMMIT') {
        snapshot = null;
        state.commits++;
        return {};
      }
      if (sql === 'SELECT 1') return { rows: [{}] };
      if (sql.startsWith('INSERT INTO jobs')) {
        const [id, filename, total_chunks, row_count] = args;
        state.jobs.set(id, {
          id,
          filename,
          total_chunks,
          row_count,
          status: 'queued',
          completed_chunks: 0,
          error: '',
          created_at: new Date(),
        });
        return { rowCount: 1 };
      }
      if (sql.startsWith('INSERT INTO chunks')) {
        const [id, job_id, object_key] = args;
        state.chunks.set(id, { id, job_id, object_key, completed: false });
        return { rowCount: 1 };
      }
      if (sql.startsWith('SELECT * FROM jobs'))
        return {
          rows: args.length
            ? [state.jobs.get(args[0])].filter(Boolean)
            : [...state.jobs.values()],
        };
      if (sql.startsWith('SELECT c.*')) {
        const c = state.chunks.get(args[0]);
        return {
          rows: c ? [{ ...c, status: state.jobs.get(c.job_id).status }] : [],
        };
      }
      if (sql.startsWith('SELECT status'))
        return { rows: [{ status: state.jobs.get(args[0]).status }] };
      if (sql.startsWith('UPDATE chunks SET completed')) {
        const c = state.chunks.get(args[0]);
        if (c.completed) return { rowCount: 0 };
        c.completed = true;
        return { rowCount: 1 };
      }
      if (sql.startsWith('INSERT INTO job_sales')) {
        const [id, region, amount] = args;
        const key = id + '|' + region;
        state.sales.set(key, (state.sales.get(key) || 0n) + BigInt(amount));
        return { rowCount: 1 };
      }
      if (sql.startsWith('UPDATE jobs SET completed_chunks')) {
        const j = state.jobs.get(args[0]);
        j.completed_chunks++;
        j.status =
          j.completed_chunks === j.total_chunks ? 'completed' : 'processing';
        return { rowCount: 1 };
      }
      if (sql.startsWith('SELECT region'))
        return {
          rows: [...state.sales]
            .filter(([key]) => key.startsWith(args[0] + '|'))
            .map(([key, n]) => ({
              region: key.split('|')[1],
              total_cents: n.toString(),
            })),
        };
      throw Error('Unhandled test query: ' + sql);
    },
  };
  const objects = {
    async putObject(bucket, key, data) {
      state.objects.set(key, Buffer.from(data));
    },
    async removeObject(bucket, key) {
      state.objects.delete(key);
    },
    async getObject(bucket, key) {
      if (!state.objects.has(key)) throw Error('missing object');
      return Readable.from([state.objects.get(key)]);
    },
    async bucketExists() {
      return true;
    },
  };
  return { db, objects, redis: null, state };
}
