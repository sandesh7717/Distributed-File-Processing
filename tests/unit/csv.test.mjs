import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import {
  cents,
  splitCSV,
  aggregate,
  validate,
  ValidationError,
} from '../../server/lib/csv.mjs';
const date = '2026-01-01T00:00:00Z';
test('decimal parsing preserves exact cents and rejects invalid values', () => {
  for (const [input, expected] of [
    ['12.34', 1234n],
    ['-9.25', -925n],
    ['1.2', 120n],
    ['0', 0n],
  ])
    assert.equal(cents(input), expected);
  for (const input of [
    'NaN',
    'Infinity',
    '1e3',
    '1.001',
    '',
    '1.',
    '+2',
    '1000000001',
  ])
    assert.throws(() => cents(input), ValidationError);
});
test('record-aligned chunks preserve quoted newlines, commas, CRLF and final row', async () => {
  const input = `id,region,amount,timestamp\r\n1,"North, East",12.34,${date}\r\n2,"South\nWest",-2.34,${date}\n3,North,0.01,${date}`;
  const chunks = [];
  const rows = await splitCSV(
    Readable.from([input]),
    async (data, count) => chunks.push({ data, count }),
    { target: 30 },
  );
  assert.equal(rows, 3);
  assert.equal(chunks.length, 3);
  let sum = 0n;
  for (const chunk of chunks) {
    assert.equal(chunk.count, 1);
    const totals = await aggregate(Readable.from([chunk.data]));
    for (const n of totals.values()) sum += n;
  }
  assert.equal(sum, 1001n);
});
test('invalid, empty and oversized CSV records are rejected', async () => {
  for (const input of [
    '',
    'id,region,amount,timestamp\n',
    'wrong,header\n',
    `id,region,amount,timestamp\n1,North,NaN,${date}`,
    `id,region,amount,timestamp\n1,North,1,2026-02-30T00:00:00Z`,
    `id,region,amount,timestamp\n1,"${'x'.repeat(2 * 1024 * 1024)}`,
  ]) {
    await assert.rejects(
      splitCSV(Readable.from([input]), async () => {}),
      ValidationError,
    );
  }
});
test('calendar validation accepts leap dates and rejects invalid offsets', () => {
  assert.equal(
    validate(['1', 'North', '1', '2024-02-29T00:00:00+05:30']),
    100n,
  );
  for (const date of [
    '2025-02-29T00:00:00Z',
    '2026-01-01T24:00:00Z',
    '2026-01-01T00:00:00+25:00',
  ])
    assert.throws(() => validate(['1', 'North', '1', date]), ValidationError);
});
test('a slow chunk sink is awaited and failures propagate', async () => {
  let active = 0,
    max = 0,
    calls = 0;
  await splitCSV(
    Readable.from([
      `id,region,amount,timestamp\n${`1,North,1,${date}\n`.repeat(10)}`,
    ]),
    async () => {
      active++;
      max = Math.max(max, active);
      await new Promise((r) => setTimeout(r, 2));
      calls++;
      active--;
    },
    { target: 1 },
  );
  assert.equal(calls, 10);
  assert.equal(max, 1);
  await assert.rejects(
    splitCSV(
      Readable.from([`id,region,amount,timestamp\n1,North,1,${date}`]),
      async () => {
        throw Error('storage failed');
      },
    ),
    /storage failed/,
  );
});
