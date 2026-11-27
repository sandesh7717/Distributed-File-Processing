import test from 'node:test';
import assert from 'node:assert/strict';
import { initializeStorage } from '../../server/lib/init-storage.mjs';

test('creates a missing bucket and removes anonymous policies', async () => {
  const calls = [];
  await initializeStorage(
    {
      bucketExists: async () => false,
      createBucket: async (b) => calls.push(['create', b]),
      makePrivate: async (b) => calls.push(['private', b]),
    },
    'uploads',
  );
  assert.deepEqual(calls, [
    ['create', 'uploads'],
    ['private', 'uploads'],
  ]);
});
test('existing bucket is reused and made private', async () => {
  let privateCalls = 0;
  await initializeStorage(
    {
      bucketExists: async () => true,
      createBucket: async () => assert.fail('must not recreate'),
      makePrivate: async () => privateCalls++,
    },
    'uploads',
  );
  assert.equal(privateCalls, 1);
});
test('startup retries transient failures and terminates after its retry budget', async () => {
  let checks = 0,
    waits = 0;
  const objects = {
    bucketExists: async () => {
      checks++;
      if (checks < 3) throw Error('starting');
      return true;
    },
    makePrivate: async () => {},
  };
  await initializeStorage(objects, 'uploads', { wait: async () => waits++ });
  assert.equal(checks, 3);
  assert.equal(waits, 2);
  await assert.rejects(
    initializeStorage(
      {
        bucketExists: async () => {
          throw Error('down');
        },
      },
      'uploads',
      { attempts: 2, wait: async () => {} },
    ),
    /bounded retries/,
  );
});
test('access-denied errors fail immediately instead of retrying forever', async () => {
  const error = Object.assign(Error('denied'), {
    $metadata: { httpStatusCode: 403 },
  });
  await assert.rejects(
    initializeStorage(
      {
        bucketExists: async () => {
          throw error;
        },
      },
      'uploads',
      { wait: async () => assert.fail('must not retry') },
    ),
    /denied/,
  );
});
