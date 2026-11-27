import { setTimeout as sleep } from 'node:timers/promises';
import { createClients, topic } from './lib/clients.mjs';
import {
  dispatchOnce,
  processChunk,
  terminalError,
  markFailed,
} from './lib/process.mjs';

const clients = createClients();
const producer = clients.kafka.producer();
const consumer = clients.kafka.consumer({
  groupId: 'file-processing-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});
const abort = new AbortController();
let dispatchTask;
async function shutdown() {
  if (abort.signal.aborted) return;
  abort.abort();
  const deadline = setTimeout(() => process.exit(1), 30000);
  deadline.unref();
  await consumer.stop().catch(() => {});
  await dispatchTask;
  await Promise.allSettled([
    consumer.disconnect(),
    producer.disconnect(),
    clients.db.end(),
  ]);
  clients.redis.destroy();
  clients.objects.close();
  clearTimeout(deadline);
}
consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (!payload.restart && !abort.signal.aborted) {
    console.error('Consumer stopped:', payload.error.message);
    process.exitCode = 1;
    shutdown().catch(console.error);
  }
});
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => shutdown().catch(console.error));
try {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  dispatchTask = (async () => {
    while (!abort.signal.aborted) {
      try {
        await dispatchOnce(clients.db, producer);
      } catch (error) {
        console.error('Dispatcher retry:', error.message);
      }
      await sleep(3000, undefined, { signal: abort.signal }).catch(() => {});
    }
  })();
  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale() || abort.signal.aborted) return;
        let task;
        try {
          task = JSON.parse(message.value.toString());
          if (typeof task.chunk_id !== 'string' || !task.chunk_id)
            throw Error('missing chunk_id');
        } catch (error) {
          console.error('Skipping malformed event:', error.message);
          task = null;
        }
        if (task) {
          let lastHeartbeat = 0;
          const beat = async () => {
            if (Date.now() - lastHeartbeat > 1000) {
              await heartbeat();
              lastHeartbeat = Date.now();
            }
          };
          let finished = false;
          while (isRunning() && !isStale() && !abort.signal.aborted) {
            // Keeps the session alive during storage/database waits, not only CSV parsing.
            let heartbeatFailure;
            const timer = setInterval(
              () =>
                heartbeat().catch((error) => {
                  heartbeatFailure = error;
                }),
              3000,
            );
            try {
              await processChunk(clients, task.chunk_id, {
                heartbeat: beat,
                signal: abort.signal,
              });
              finished = true;
            } catch (error) {
              if (terminalError(error)) {
                try {
                  await markFailed(clients, task.chunk_id);
                  finished = true;
                } catch (markError) {
                  console.error('Failure update retry:', markError.message);
                }
              } else if (!abort.signal.aborted)
                console.error('Chunk retry:', task.chunk_id, error.message);
            } finally {
              clearInterval(timer);
            }
            if (heartbeatFailure) throw heartbeatFailure;
            if (finished) break;
            await heartbeat();
            await sleep(1000, undefined, { signal: abort.signal }).catch(
              () => {},
            );
          }
          if (!finished) return;
        }
        if (!isRunning() || isStale() || abort.signal.aborted) return;
        resolveOffset(message.offset);
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        await heartbeat();
      }
    },
  });
  console.log('JavaScript worker ready');
} catch (error) {
  console.error(error);
  await shutdown();
  process.exitCode = 1;
}
