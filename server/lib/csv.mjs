import { parse } from 'csv-parse';
import { pipeline } from 'node:stream/promises';

export const CHUNK_BYTES = 4 * 1024 * 1024;
export const MAX_CENTS = 9223372036854775807n;
export class ValidationError extends Error {
  status = 400;
}
export function cents(value) {
  const text = value.trim();
  if (!/^-?\d+(?:\.\d{1,2})?$/.test(text))
    throw new ValidationError(
      'Amount must be a decimal with at most two fractional digits',
    );
  const negative = text.startsWith('-');
  const [whole, fraction = ''] = text.replace(/^-/, '').split('.');
  if (BigInt(whole) > 1000000000n)
    throw new ValidationError('Amount exceeds 1 billion');
  const amount = BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0'));
  return negative ? -amount : amount;
}
export function validate(row) {
  if (row.length !== 4)
    throw new ValidationError('Expected id,region,amount,timestamp');
  if (!row[0].trim() || !row[1].trim() || Buffer.byteLength(row[1]) > 100)
    throw new ValidationError(
      'ID and region are required; region must be at most 100 bytes',
    );
  // Validate the calendar date explicitly: Date.parse normalizes dates such as February 30.
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-](\d{2}):(\d{2}))$/.exec(
      row[3],
    );
  if (!match) throw new ValidationError('Timestamp must be RFC3339');
  const [, y, m, d, h, min, s, , oh = '0', om = '0'] = match;
  const year = Number(y),
    month = Number(m),
    day = Number(d);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > days[month - 1] ||
    +h > 23 ||
    +min > 59 ||
    +s > 59 ||
    +oh > 23 ||
    +om > 59
  )
    throw new ValidationError('Invalid RFC3339 timestamp');
  return cents(row[2]);
}
export function encodeRow(row) {
  return (
    row
      .map((value) =>
        /[",\r\n]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value,
      )
      .join(',') + '\n'
  );
}
// pipeline() forwards source failures and enforces backpressure in both directions.
export async function readRecords(
  input,
  visit,
  { header = false, signal } = {},
) {
  let first = header,
    count = 0;
  try {
    await pipeline(
      input,
      parse({
        bom: true,
        record_delimiter: ['\r\n', '\n', '\r'],
        max_record_size: 1024 * 1024,
        relax_column_count: true,
        skip_empty_lines: true,
      }),
      async (records) => {
        for await (const row of records) {
          if (first) {
            first = false;
            if (
              row.length !== 4 ||
              row.some(
                (v, i) =>
                  v.trim() !== ['id', 'region', 'amount', 'timestamp'][i],
              )
            )
              throw new ValidationError(
                'Expected header: id,region,amount,timestamp',
              );
            continue;
          }
          const amount = validate(row);
          await visit(row, amount);
          count++;
        }
      },
      { signal },
    );
  } catch (error) {
    if (error.code?.startsWith('CSV_'))
      throw new ValidationError(error.message);
    throw error;
  }
  if (header && !count) throw new ValidationError('CSV contains no data rows');
  return count;
}
export async function splitCSV(
  input,
  emit,
  { target = CHUNK_BYTES, signal } = {},
) {
  let parts = [],
    size = 0,
    rows = 0;
  async function flush() {
    if (!rows) return;
    await emit(Buffer.concat(parts, size), rows);
    parts = [];
    size = 0;
    rows = 0;
  }
  const total = await readRecords(
    input,
    async (row) => {
      const data = Buffer.from(encodeRow(row));
      parts.push(data);
      size += data.length;
      rows++;
      if (size >= target) await flush();
    },
    { header: true, signal },
  );
  await flush();
  return total;
}
export async function aggregate(input, options = {}) {
  const totals = new Map();
  await readRecords(
    input,
    async (row, amount) => {
      const total = (totals.get(row[1]) ?? 0n) + amount;
      if (total > MAX_CENTS || total < -MAX_CENTS - 1n)
        throw new ValidationError('Regional total exceeds BIGINT range');
      totals.set(row[1], total);
      await options.heartbeat?.();
    },
    options,
  );
  return totals;
}
