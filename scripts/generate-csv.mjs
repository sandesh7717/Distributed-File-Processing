import { createWriteStream } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { once } from 'node:events';
const [file, bytesArg] = process.argv.slice(2);
if (!file || !/^\d+$/.test(bytesArg || '')) {
  console.error('Usage: npm run generate:csv -- /path/sales.csv TARGET_BYTES');
  process.exit(1);
}
const target = Number(bytesArg);
if (!Number.isSafeInteger(target) || target < 100)
  throw Error('TARGET_BYTES must be a safe integer of at least 100');
const out = createWriteStream(file, { flags: 'wx' }); // Never overwrite an existing benchmark file.
let failure;
out.on('error', (error) => {
  failure = error;
});
let bytes = 0,
  rows = 0;
const totals = { North: 0n, South: 0n, West: 0n };
const regions = Object.keys(totals);
async function write(text) {
  if (failure) throw failure;
  bytes += Buffer.byteLength(text);
  if (!out.write(text)) await once(out, 'drain');
}
try {
  await write('id,region,amount,timestamp\n');
  while (bytes < target) {
    let block = '',
      blockBytes = 0;
    for (let i = 0; i < 5000 && bytes + blockBytes < target; i++) {
      const region = regions[rows % 3];
      rows++;
      const line = `${rows},${region},1.25,2026-01-01T00:00:00Z\n`;
      block += line;
      blockBytes += Buffer.byteLength(line);
      totals[region] += 125n;
    }
    await write(block);
  }
  out.end();
  await once(out, 'finish');
  const manifest = {
    bytes,
    rows,
    total_cents: Object.fromEntries(
      Object.entries(totals).map(([k, v]) => [k, v.toString()]),
    ),
  };
  await writeFile(file + '.metrics.json', JSON.stringify(manifest, null, 2), {
    flag: 'wx',
  });
  console.log(JSON.stringify(manifest, null, 2));
} catch (error) {
  out.destroy();
  throw error;
}
