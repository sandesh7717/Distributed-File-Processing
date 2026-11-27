import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { upload, UploadError } from './lib/upload.mjs';
import { ValidationError } from './lib/csv.mjs';
import { bucket } from './lib/clients.mjs';
import { loadJob, jobJSON } from './lib/jobs.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export function createApp(clients, options = {}) {
  const { db, objects, redis } = clients;
  const maxBytes =
    options.maxBytes ?? Number(process.env.MAX_UPLOAD_BYTES || 16 * 1024 ** 3);
  if (
    !Number.isSafeInteger(maxBytes) ||
    maxBytes <= 0 ||
    maxBytes >= Number.MAX_SAFE_INTEGER
  )
    throw Error('MAX_UPLOAD_BYTES must be a positive safe integer');
  options = { ...options, maxBytes };
  const app = express();
  app.disable('x-powered-by');
  app.use((req, res, next) => {
    res.set({
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'same-origin',
      'X-Frame-Options': 'DENY',
      // Next static export includes inline bootstrap scripts. No arbitrary HTML is rendered.
      'Content-Security-Policy':
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'; frame-ancestors 'none'; base-uri 'self'",
    });
    if (req.path.startsWith('/api/')) res.set('Cache-Control', 'no-store');
    next();
  });
  app.get('/healthz', async (req, res) => {
    await db.query('SELECT 1');
    if (!(await objects.bucketExists(bucket)))
      throw Error('Storage bucket unavailable');
    res.json({
      status: 'ok',
      redis: redis?.isReady ? 'connected' : 'unavailable; PostgreSQL fallback',
    });
  });
  app.get('/api/config', (req, res) =>
    res.json({
      max_upload_bytes:
        options.maxBytes ||
        Number(process.env.MAX_UPLOAD_BYTES || 16 * 1024 ** 3),
    }),
  );
  app.post(['/api/jobs', '/upload'], async (req, res) =>
    res.status(202).json(await upload(req, clients, options)),
  );
  app.get(['/api/jobs', '/jobs'], async (req, res) => {
    const { rows } = await db.query(
      'SELECT * FROM jobs ORDER BY created_at DESC LIMIT 100',
    );
    res.json(rows.map(jobJSON));
  });
  const validateID = (req, res, next) => {
    if (
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        req.params.id,
      )
    )
      return res.status(400).json({ error: 'Invalid job ID' });
    next();
  };
  app.get(['/api/jobs/:id', '/status/:id'], validateID, async (req, res) => {
    const job = await loadJob(db, redis, req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
  });
  app.get(
    ['/api/jobs/:id/results', '/results/:id'],
    validateID,
    async (req, res) => {
      const job = await loadJob(db, null, req.params.id, { cached: false });
      if (!job) return res.status(404).json({ error: 'Job not found' });
      if (job.status !== 'completed')
        return res.status(409).json({
          error:
            job.status === 'failed'
              ? 'Processing failed'
              : 'Results are available when processing completes',
        });
      const { rows } = await db.query(
        'SELECT region,total_cents::text AS total_cents FROM job_sales WHERE job_id=$1 ORDER BY job_sales.total_cents DESC,region',
        [req.params.id],
      );
      res.json(rows);
    },
  );
  app.use('/api', (req, res) =>
    res.status(404).json({ error: 'API route not found' }),
  );
  app.use(
    express.static(options.staticDir || path.join(root, 'out'), {
      extensions: ['html'],
    }),
  );
  app.use((req, res) => res.status(404).json({ error: 'Not found' }));
  app.use((error, req, res, next) => {
    if (res.headersSent) return next(error);
    if (error instanceof UploadError || error instanceof ValidationError)
      return res.status(error.status).json({ error: error.message });
    console.error('Request failed:', error.message);
    res.status(503).json({ error: 'Service temporarily unavailable' });
  });
  return app;
}
