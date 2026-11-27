import Busboy from 'busboy';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { splitCSV } from './csv.mjs';
import { bucket } from './clients.mjs';

export class UploadError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}
export async function upload(
  req,
  { db, objects },
  {
    maxBytes = Number(process.env.MAX_UPLOAD_BYTES || 16 * 1024 ** 3),
    chunkBytes,
  } = {},
) {
  const id = randomUUID(),
    keys = [];
  let durable = false,
    filename,
    recordCount = 0,
    task,
    seen = false;
  const cancellation = new AbortController();
  const aborted = () => cancellation.abort();
  req.once('aborted', aborted);
  let parser;
  try {
    try {
      parser = Busboy({
        headers: req.headers,
        limits: {
          files: 1,
          fields: 0,
          parts: 2,
          // Busboy emits limit when the threshold is reached; allow exactly maxBytes.
          fileSize: maxBytes + 1,
          headerPairs: 100,
        },
      });
    } catch {
      throw new UploadError('Use multipart/form-data with a file field');
    }
    await new Promise((resolve, reject) => {
      let settled = false;
      const fail = (error) => {
        if (settled) return;
        settled = true;
        req.unpipe(parser);
        req.resume();
        queueMicrotask(() => {
          parser.destroy();
          cancellation.abort();
        });
        reject(error);
      };
      req.once('error', fail);
      parser.on('error', (error) => fail(new UploadError(error.message)));
      parser.on('filesLimit', () =>
        fail(new UploadError('Upload one file at a time')),
      );
      parser.on('fieldsLimit', () =>
        fail(new UploadError('Only the file field is accepted')),
      );
      parser.on('partsLimit', () =>
        fail(new UploadError('Only one file field is accepted')),
      );
      parser.on('file', (name, file, info) => {
        file.on('error', fail);
        seen = true;
        filename = path.basename(info.filename || '');
        if (name !== 'file' || !filename.toLowerCase().endsWith('.csv')) {
          file.resume();
          fail(new UploadError('Choose a .csv file in the file field'));
          return;
        }
        file.on('limit', () =>
          fail(new UploadError('File exceeds upload limit', 413)),
        );
        task = splitCSV(
          file,
          async (data) => {
            const key = `${id}/${keys.length}.csv`;
            keys.push(key);
            try {
              await objects.putObject(bucket, key, data, data.length, {
                'Content-Type': 'text/csv',
              });
            } catch (error) {
              console.error('Storage upload:', error.message);
              throw new UploadError(
                'Storage unavailable; please try again',
                503,
              );
            }
          },
          { target: chunkBytes, signal: cancellation.signal },
        ).then((count) => {
          recordCount = count;
        });
        task.catch(fail);
      });
      parser.on('close', () => {
        req.removeListener('error', fail);
        if (!settled) {
          settled = true;
          resolve();
        }
      });
      req.pipe(parser);
    });
    if (!seen) throw new UploadError('No file uploaded');
    await task;
    const tx = await db.connect();
    try {
      await tx.query('BEGIN');
      await tx.query(
        'INSERT INTO jobs(id,filename,total_chunks,row_count) VALUES($1,$2,$3,$4)',
        [id, filename, keys.length, recordCount],
      );
      for (const [index, key] of keys.entries())
        await tx.query(
          'INSERT INTO chunks(id,job_id,object_key) VALUES($1,$2,$3)',
          [`${id}-${index}`, id, key],
        );
      // A lost COMMIT response is ambiguous: retain objects for a possibly committed job.
      durable = true;
      await tx.query('COMMIT');
    } catch (error) {
      await tx.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      tx.release();
    }
    return { job_id: id, message: 'File accepted for processing' };
  } finally {
    req.removeListener('aborted', aborted);
    // Wait for any in-flight storage operation before deleting staged objects.
    await task?.catch(() => {});
    if (!durable)
      for (const key of keys)
        await objects
          .removeObject(bucket, key)
          .catch((error) =>
            console.error('Orphan cleanup:', key, error.message),
          );
  }
}
