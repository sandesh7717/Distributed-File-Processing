import { createObjectStore, bucket } from './lib/clients.mjs';
import { initializeStorage } from './lib/init-storage.mjs';
const objects = createObjectStore();
try {
  await initializeStorage(objects, bucket);
  console.log('Private uploads bucket is ready');
} catch (error) {
  console.error(error.message, error.cause?.message || '');
  process.exitCode = 1;
} finally {
  objects.close();
}
