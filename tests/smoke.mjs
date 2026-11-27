import assert from 'node:assert/strict';
const base = process.env.BASE_URL || 'http://127.0.0.1:8080';
const response = await fetch(base + '/healthz');
assert.equal(response.status, 200);
const csv =
  'id,region,amount,timestamp\n1,North,125.50,2026-01-01T10:00:00Z\n2,South,89.25,2026-01-01T11:00:00Z\n3,North,74.50,2026-01-01T12:00:00Z\n4,South,-9.25,2026-01-01T14:00:00Z\n';
async function upload(text, name = 'sales.csv') {
  const body = new FormData();
  body.append('file', new Blob([text], { type: 'text/csv' }), name);
  return fetch(base + '/api/jobs', { method: 'POST', body });
}
assert.equal((await upload('bad')).status, 400);
assert.equal((await upload(csv, 'sales.txt')).status, 400);
const uploaded = await upload(csv);
assert.equal(uploaded.status, 202, await uploaded.clone().text());
const { job_id } = await uploaded.json();
let job;
for (let i = 0; i < 90; i++) {
  job = await (await fetch(base + '/api/jobs/' + job_id)).json();
  if (job.status === 'completed') break;
  await new Promise((r) => setTimeout(r, 1000));
}
assert.equal(job.status, 'completed');
assert.equal(job.rows, 4);
assert.equal(job.progress, 100);
const results = await (
  await fetch(base + '/api/jobs/' + job_id + '/results')
).json();
assert.deepEqual(results, [
  { region: 'North', total_cents: '20000' },
  { region: 'South', total_cents: '8000' },
]);
assert.ok(
  (await (await fetch(base + '/api/jobs')).json()).some(
    (j) => j.job_id === job_id,
  ),
);
console.log(
  'PASS: health, upload validation, Kafka processing, status, exact totals, job history',
);

// A second real upload spans multiple chunks and uses independent Kafka tasks.
const count = 140000;
const multi =
  'id,region,amount,timestamp\n' +
  '1,North,1.25,2026-01-01T00:00:00Z\n'.repeat(count);
const large = await upload(multi, 'multi-chunk.csv');
assert.equal(large.status, 202, await large.clone().text());
const { job_id: multiId } = await large.json();
let multiJob;
for (let i = 0; i < 120; i++) {
  multiJob = await (await fetch(`${base}/api/jobs/${multiId}`)).json();
  if (multiJob.status === 'completed' || multiJob.status === 'failed') break;
  await new Promise((r) => setTimeout(r, 1000));
}
assert.equal(multiJob.status, 'completed');
assert.ok(multiJob.total_chunks > 1);
assert.equal(multiJob.rows, count);
assert.deepEqual(
  await (await fetch(`${base}/api/jobs/${multiId}/results`)).json(),
  [{ region: 'North', total_cents: (BigInt(count) * 125n).toString() }],
);
console.log('PASS: real multi-chunk upload and exact regional totals');
