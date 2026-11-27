# JavaScript migration verification — 28 November 2026

## Passed locally

- **`npm test`: 20 tests passed.** These cover streamed CSV parsing and record-aligned chunking, quoted commas/newlines, mixed line endings, invalid dates/amounts, oversized records, exact upload limits, backpressure, HTTP upload through Express, durable job creation, staged-object cleanup, ambiguous commit handling, duplicate processing, transaction rollback/retry, outbox acknowledgment order, Redis fallback, and storage initialization (create/reuse, privacy, bounded retries, authorization failures).
- **`npm run test:browser`: 5 tests passed** using Chromium against the actual Express-served Next.js production export. Upload/progress/results, recoverable errors, mobile layout, safe filename rendering, desktop rendering, and exact large-number formatting are covered.
- **`npm run build`: passed**, producing a Next.js static export in `out/`.
- **`npm run format:check`: passed.** JavaScript/React/CSS source is formatted for readability.
- Compose, integration Compose, and CI YAML parsed successfully. The dependency lock matches `package.json`; `git diff --check` passed.
- The dependency installation audit reported **0 vulnerabilities** after replacing the MinIO JavaScript client with the S3-compatible AWS SDK. This is a dependency audit, not a security assessment of the complete application or container images.
- The streaming dataset generator successfully created a 100,010-byte CSV with 2,757 rows and an expected-results manifest.

## What these checks do not prove

The Node API/worker tests use an in-memory database/storage test double. They do not implement real PostgreSQL locking or prove S3 protocol interoperability. Browser tests intercept job/results responses; they verify the frontend against the expected REST contract, not a live Kafka pipeline.

**Docker is not installed in this environment.** Consequently, neither the container images nor Compose startup, real-service smoke tests, Redis Lua execution, PostgreSQL concurrent locking tests, or Kafka worker failover were executed locally. The repository includes real-service tests and CI configuration, but no remote CI execution was triggered or observed during this task. YAML parsing does not replace `docker compose config` or a real startup.

No 10GB+ benchmark, sustained load test, or cloud deployment was performed. The 16 GiB file limit is a configuration value, not a demonstrated capacity result.

## Run the remaining checks on a Docker host

```sh
cp .env.example .env
# Replace the password values.
docker compose up -d --build --scale worker=3 --wait --wait-timeout 240
npm run test:smoke
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm integration
```

`test:smoke` sends real uploads through the REST API and Kafka workers, including a multi-chunk CSV, and checks exact totals. The integration container exercises concurrent duplicate delivery against PostgreSQL, object reads from MinIO, and Redis's monotonic cache script and database fallback.

For failure recovery acceptance, upload a larger dataset, kill one worker during processing, and confirm remaining workers finish with the exact expected totals. Record resource usage and timing rather than inferring scalability from the code.

## CI follow-up: unavailable MinIO images

After pinning the CI runner to Ubuntu 24.04, browser tests passed on GitHub. The next failure was anonymous pull denial for `minio/mc`. Direct checks also found that both exact release manifests on Quay returned 401 after the public registry token flow.

The stack now builds the same MinIO release from official GitHub source, verified against SHA-256 `1a63209927b9d1fb239b2a776b7b01d1902eff19379a108f4bc7005edcc0968c`, and initializes its private bucket with the existing JavaScript S3 client. All 20 local Node tests pass. The MinIO container build and real-service checks still require confirmation from the new CI run; the first source build takes longer than an image pull.
