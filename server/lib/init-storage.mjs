import { setTimeout as sleep } from 'node:timers/promises';

export async function initializeStorage(
  objects,
  bucket,
  { attempts = 30, delayMs = 2000, wait = sleep } = {},
) {
  let lastError;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      if (!(await objects.bucketExists(bucket)))
        await objects.createBucket(bucket);
      await objects.makePrivate(bucket);
      return;
    } catch (error) {
      lastError = error;
      // Credentials or authorization failures require configuration changes.
      if (error.$metadata?.httpStatusCode === 403) throw error;
      if (attempt + 1 < attempts) await wait(delayMs);
    }
  }
  throw new Error('Storage initialization failed after bounded retries', {
    cause: lastError,
  });
}
