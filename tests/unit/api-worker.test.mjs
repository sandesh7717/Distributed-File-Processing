import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createApp } from '../../server/app.mjs';
import { processChunk, dispatchOnce } from '../../server/lib/process.mjs';
import { fixtures } from './helpers.mjs';
const csv =
  'id,region,amount,timestamp\n1,North,125.50,2026-01-01T00:00:00Z\n2,South,80,2026-01-01T00:00:00Z\n3,North,74.50,2026-01-01T00:00:00Z\n';
async function setup(t, options = {}) {
  const clients = fixtures();
  const server = createApp(clients, options).listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((r) => server.close(r)));
  const base = `http://127.0.0.1:${server.address().port}`;
  return { ...clients, base };
}
async function send(base, text = csv, name = 'sales.csv') {
  const body = new FormData();
  body.append('file', new Blob([text]), name);
  return fetch(base + '/api/jobs', { method: 'POST', body });
}
test('REST upload, chunk processing, exact results and duplicate delivery', async (t) => {
  const f = await setup(t, { chunkBytes: 1 });
  const response = await send(f.base);
  assert.equal(response.status, 202, await response.clone().text());
  const { job_id } = await response.json();
  assert.equal(f.state.chunks.size, 3);
  assert.equal(
    (await fetch(`${f.base}/api/jobs/${job_id}/results`)).status,
    409,
  );
  for (const id of f.state.chunks.keys()) await processChunk(f, id);
  for (const id of f.state.chunks.keys()) await processChunk(f, id);
  const job = await (await fetch(`${f.base}/api/jobs/${job_id}`)).json();
  assert.equal(job.progress, 100);
  assert.equal(job.completed_chunks, 3);
  assert.equal(job.rows, 3);
  assert.deepEqual(
    await (await fetch(`${f.base}/api/jobs/${job_id}/results`)).json(),
    [
      { region: 'North', total_cents: '20000' },
      { region: 'South', total_cents: '8000' },
    ],
  );
});
test('bad file, malformed CSV, invalid IDs and oversized uploads', async (t) => {
  const f = await setup(t, { maxBytes: 300 });
  assert.equal((await send(f.base, csv, 'sales.txt')).status, 400);
  assert.equal((await send(f.base, 'invalid')).status, 400);
  assert.equal((await send(f.base, csv.repeat(10))).status, 413);
  assert.equal((await fetch(f.base + '/api/jobs/nope')).status, 400);
  assert.equal(f.state.jobs.size, 0);
  assert.equal(f.state.objects.size, 0);
});
test('database failure rolls back job creation and removes staged objects', async (t) => {
  const f = await setup(t);
  f.state.fail = 'INSERT INTO chunks';
  assert.equal((await send(f.base)).status, 503);
  assert.equal(f.state.jobs.size, 0);
  assert.equal(f.state.objects.size, 0);
  assert.equal(f.state.rollbacks, 1);
});
test('worker failure rolls back completion; retry applies totals once', async (t) => {
  const f = await setup(t);
  assert.equal((await send(f.base)).status, 202);
  const id = [...f.state.chunks.keys()][0];
  f.state.fail = 'INSERT INTO job_sales';
  await assert.rejects(processChunk(f, id), /injected/);
  assert.equal(f.state.chunks.get(id).completed, false);
  assert.equal(f.state.sales.size, 0);
  f.state.fail = null;
  await processChunk(f, id);
  assert.equal(
    f.state.sales.get([...f.state.jobs.keys()][0] + '|North'),
    20000n,
  );
});
test('outbox marks publication only after Kafka acknowledges', async () => {
  const calls = [];
  const db = {
    query: async (sql) => {
      calls.push(sql);
      return { rows: [{ id: 'a' }] };
    },
  };
  await assert.rejects(
    dispatchOnce(db, {
      send: async () => {
        throw Error('Kafka down');
      },
    }),
    /Kafka down/,
  );
  assert.equal(calls.length, 1);
  calls.length = 0;
  await dispatchOnce(db, {
    send: async (request) => {
      assert.equal(request.messages[0].value, '{"chunk_id":"a"}');
      calls.push('sent');
    },
  });
  assert.equal(calls[1], 'sent');
  assert.match(calls[2], /UPDATE chunks/);
});
test('multiple file fields and missing file are rejected', async (t) => {
  const f = await setup(t);
  const body = new FormData();
  body.append('file', new Blob([csv]), 'a.csv');
  body.append('file', new Blob([csv]), 'b.csv');
  assert.equal(
    (await fetch(f.base + '/api/jobs', { method: 'POST', body })).status,
    400,
  );
  assert.equal(f.state.jobs.size, 0);
  assert.equal(
    (
      await fetch(f.base + '/api/jobs', {
        method: 'POST',
        body: new FormData(),
      })
    ).status,
    400,
  );
});

test('exact upload size limit is accepted; one byte beyond is rejected', async (t) => {
  const exact = await setup(t, { maxBytes: Buffer.byteLength(csv) });
  assert.equal((await send(exact.base)).status, 202);
  const smaller = await setup(t, { maxBytes: Buffer.byteLength(csv) - 1 });
  assert.equal((await send(smaller.base)).status, 413);
});
test('uncertain commit keeps stored objects for a possibly committed job', async (t) => {
  const f = await setup(t);
  f.state.fail = 'COMMIT';
  assert.equal((await send(f.base)).status, 503);
  assert.ok(f.state.objects.size > 0);
});
test('validation failure after a staged chunk removes all staged objects', async (t) => {
  const f = await setup(t, { chunkBytes: 1 });
  assert.equal(
    (await send(f.base, csv + '4,North,invalid,2026-01-01T00:00:00Z\n')).status,
    400,
  );
  assert.equal(f.state.objects.size, 0);
  assert.equal(f.state.jobs.size, 0);
});
