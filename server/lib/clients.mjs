import pg from 'pg';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadBucketCommand,
  CreateBucketCommand,
  DeleteBucketPolicyCommand,
} from '@aws-sdk/client-s3';
import { createClient } from 'redis';
import { Kafka, logLevel } from 'kafkajs';

export const topic = 'file-chunks';
export const bucket = process.env.MINIO_BUCKET || 'uploads';
export function createClients() {
  const db = new pg.Pool({
    connectionString:
      process.env.DATABASE_URL ||
      'postgres://sde_user:sde_password@localhost:5432/sales_db',
    max: 12,
    connectionTimeoutMillis: 5000,
    statement_timeout: 30000,
  });
  db.on('error', (error) =>
    console.error('Idle database connection:', error.message),
  );
  const objects = createObjectStore();
  const redis = createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
    disableOfflineQueue: true,
    socket: {
      connectTimeout: 1000,
      reconnectStrategy: (retries) => Math.min(1000 + retries * 200, 5000),
    },
  });
  redis.on('error', () => {}); // Redis is optional; PostgreSQL remains authoritative.
  redis.connect().catch(() => {});
  const kafka = new Kafka({
    clientId: 'flow-js',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    logLevel: logLevel.WARN,
    retry: { retries: 5 },
  });
  return { db, objects, redis, kafka };
}

// Shared by the API, workers and one-shot storage initialization.
export function createObjectStore() {
  const endpoint = new URL(
    `${process.env.MINIO_SECURE === 'true' ? 'https' : 'http'}://${process.env.MINIO_ENDPOINT || 'localhost:9000'}`,
  );
  const s3 = new S3Client({
    endpoint: endpoint.href,
    region: process.env.S3_REGION || 'us-east-1',
    forcePathStyle: true,
    credentials: {
      accessKeyId: process.env.MINIO_USER || 'minioadmin',
      secretAccessKey: process.env.MINIO_PASSWORD || 'minioadmin',
    },
    maxAttempts: 3,
  });
  const objects = {
    async putObject(Bucket, Key, Body, ContentLength) {
      await s3.send(
        new PutObjectCommand({
          Bucket,
          Key,
          Body,
          ContentLength,
          ContentType: 'text/csv',
        }),
        { abortSignal: AbortSignal.timeout(120000) },
      );
    },
    async getObject(Bucket, Key) {
      return (
        await s3.send(new GetObjectCommand({ Bucket, Key }), {
          abortSignal: AbortSignal.timeout(120000),
        })
      ).Body;
    },
    async removeObject(Bucket, Key) {
      await s3.send(new DeleteObjectCommand({ Bucket, Key }), {
        abortSignal: AbortSignal.timeout(10000),
      });
    },
    async bucketExists(Bucket) {
      try {
        await s3.send(new HeadBucketCommand({ Bucket }), {
          abortSignal: AbortSignal.timeout(5000),
        });
        return true;
      } catch (error) {
        if (error.$metadata?.httpStatusCode === 404) return false;
        throw error;
      }
    },
    async createBucket(Bucket) {
      try {
        await s3.send(new CreateBucketCommand({ Bucket }), {
          abortSignal: AbortSignal.timeout(5000),
        });
      } catch (error) {
        if (error.name !== 'BucketAlreadyOwnedByYou') throw error;
      }
    },
    async makePrivate(Bucket) {
      try {
        await s3.send(new DeleteBucketPolicyCommand({ Bucket }), {
          abortSignal: AbortSignal.timeout(5000),
        });
      } catch (error) {
        if (error.name !== 'NoSuchBucketPolicy') throw error;
      }
    },
    close() {
      s3.destroy();
    },
  };
  return objects;
}
