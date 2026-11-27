import { createClients } from './lib/clients.mjs';
import { createApp } from './app.mjs';
const clients = createClients();
const server = createApp(clients).listen(
  Number(process.env.PORT || 8080),
  process.env.HTTP_HOST || '0.0.0.0',
  () =>
    console.log(
      `Flow API and Next.js frontend: http://localhost:${process.env.PORT || 8080}`,
    ),
);
// Large uploads may take longer than Node's default request timeout.
server.requestTimeout = Number(process.env.UPLOAD_TIMEOUT_MS || 3600000);
server.headersTimeout = 15000;
let closing = false;
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => {
    if (closing) return;
    closing = true;
    const deadline = setTimeout(() => process.exit(1), 30000);
    deadline.unref();
    server.close(async () => {
      await clients.db.end();
      clients.redis.destroy();
      clients.objects.close();
      clearTimeout(deadline);
    });
  });
