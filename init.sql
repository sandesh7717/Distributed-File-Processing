CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, filename TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'queued', total_chunks INTEGER NOT NULL,
 completed_chunks INTEGER NOT NULL DEFAULT 0, row_count BIGINT NOT NULL,
 error TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS chunks (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id), object_key TEXT NOT NULL,
 completed BOOLEAN NOT NULL DEFAULT false, published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS chunks_pending ON chunks(published_at) WHERE NOT completed;
CREATE TABLE IF NOT EXISTS job_sales (
 job_id TEXT NOT NULL REFERENCES jobs(id), region TEXT NOT NULL,
 total_cents BIGINT NOT NULL, PRIMARY KEY(job_id,region)
);
