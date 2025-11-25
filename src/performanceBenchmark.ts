import { MongoClient } from 'mongodb';
import { createClient } from 'redis';

const OPERATIONS = 10000;

interface BenchmarkResult {
  database: string;
  operation: string;
  duration: number;
  opsPerSec: number;
}

async function getClients() {
  await new Promise(resolve => setTimeout(resolve, 3000));

  const redisClient = createClient({
    url: 'redis://redis:6379'
  });
  await redisClient.connect();

  const mongoClient = new MongoClient('mongodb://mongo1:27017,mongo2:27017,mongo3:27017,mongo4:27017,mongo5:27017,mongo6:27017,mongo7:27017/?replicaSet=rs0');
  await mongoClient.connect();
  const db = mongoClient.db('benchmark_db');
  const collection = db.collection('test_collection');

  await redisClient.flushAll();
  await collection.deleteMany({});

  return { redisClient, collection, mongoClient };
}

export async function runPerformanceBenchmark(): Promise<BenchmarkResult[]> {
  const { redisClient, collection, mongoClient } = await getClients();
  const results: BenchmarkResult[] = [];

  // Redis Write
  let start = Date.now();
  const redisPipeline = redisClient.multi();
  for (let i = 0; i < OPERATIONS; i++) {
    redisPipeline.set(`key:${i}`, `value:${i}`);
  }
  await redisPipeline.exec();
  let duration = (Date.now() - start) / 1000;
  results.push({
    database: 'Redis',
    operation: 'Write',
    duration,
    opsPerSec: Math.round(OPERATIONS / duration)
  });

  // MongoDB Bulk Write
  start = Date.now();
  const docs = [];
  for (let i = 0; i < OPERATIONS; i++) {
    docs.push({ key: i, value: `value:${i}` });
  }
  await collection.insertMany(docs);
  duration = (Date.now() - start) / 1000;
  results.push({
    database: 'MongoDB',
    operation: 'Bulk Write',
    duration,
    opsPerSec: Math.round(OPERATIONS / duration)
  });

  // Redis Read
  start = Date.now();
  const redisReadPipeline = redisClient.multi();
  for (let i = 0; i < OPERATIONS; i++) {
    redisReadPipeline.get(`key:${i}`);
  }
  await redisReadPipeline.exec();
  duration = (Date.now() - start) / 1000;
  results.push({
    database: 'Redis',
    operation: 'Read',
    duration,
    opsPerSec: Math.round(OPERATIONS / duration)
  });

  // MongoDB Read
  start = Date.now();
  for (let i = 0; i < OPERATIONS; i++) {
    await collection.findOne({ key: i });
  }
  duration = (Date.now() - start) / 1000;
  results.push({
    database: 'MongoDB',
    operation: 'Read',
    duration,
    opsPerSec: Math.round(OPERATIONS / duration)
  });

  await redisClient.quit();
  await mongoClient.close();

  return results;
}
