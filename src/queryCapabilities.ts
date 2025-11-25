import { MongoClient } from 'mongodb';
import { createClient } from 'redis';
import { sampleUsers } from './sampleData';

interface QueryResult {
  success: boolean;
  result?: any;
  error?: string;
  executionTime?: number;
}

interface QueryResponse {
  redis: QueryResult;
  mongodb: QueryResult;
}

async function getClients() {
  await new Promise(resolve => setTimeout(resolve, 1000));

  const redisClient = createClient({
    url: 'redis://redis:6379'
  });
  await redisClient.connect();

  const mongoClient = new MongoClient('mongodb://mongo1:27017,mongo2:27017,mongo3:27017,mongo4:27017,mongo5:27017,mongo6:27017,mongo7:27017/?replicaSet=rs0');
  await mongoClient.connect();
  const db = mongoClient.db('query_demo_db');
  const collection = db.collection('users');

  return { redisClient, collection, mongoClient };
}

async function setupData() {
  const { redisClient, collection, mongoClient } = await getClients();

  await redisClient.flushAll();
  await collection.drop().catch(() => {});

  // Setup MongoDB data
  await collection.insertMany(sampleUsers as any);
  console.log(sampleUsers)
  // Setup Redis data 
  for (let i = 0; i < sampleUsers.length; i++) {
    const user = sampleUsers[i];
    await redisClient.set(`user:${user._id}`, JSON.stringify(user));
  }
  await redisClient.set('counter', '0');
  await redisClient.lPush('queue', ['item1', 'item2', 'item3']);
  await redisClient.quit();
  await mongoClient.close();
}

export async function initializeData(): Promise<{ success: boolean }> {
  await setupData();
  return { success: true };
}

export async function executeQuery(queryType: string): Promise<QueryResponse> {
  const { redisClient, collection, mongoClient } = await getClients();
  const response: QueryResponse = {
    redis: { success: false },
    mongodb: { success: false }
  };

  try {
    switch (queryType) {
      case 'get-by-id':
        // Redis
        const redisStart1 = Date.now();
        const redisUser = await redisClient.get('user:1');
        response.redis = {
          success: true,
          result: redisUser ? JSON.parse(redisUser) : null,
          executionTime: Date.now() - redisStart1
        };

        // MongoDB
        const mongoStart1 = Date.now();
        const mongoUser = await collection.findOne({ _id: 1 } as any);
        response.mongodb = {
          success: true,
          result: mongoUser,
          executionTime: Date.now() - mongoStart1
        };
        break;

      case 'range-query':
        // Redis - not supported natively
        response.redis = {
          success: false,
          error: 'Range queries not supported without additional data structures'
        };

        // MongoDB
        const mongoStart2 = Date.now();
        const users = await collection.find({ age: { $gte: 43, $lte: 79 } }).toArray();
        response.mongodb = {
          success: true,
          result: users,
          executionTime: Date.now() - mongoStart2
        };
        break;

      case 'sort':
        // Redis - not supported
        response.redis = {
          success: false,
          error: 'Use sorted sets for specific sorting needs'
        };

        // MongoDB
        const mongoStart4 = Date.now();
        const sorted = await collection.find().sort({ score: -1 }).toArray();
        response.mongodb = {
          success: true,
          result: sorted,
          executionTime: Date.now() - mongoStart4
        };
        break;

      case 'aggregate':
        // Redis - not supported
        response.redis = {
          success: false,
          error: 'Aggregation must be done in application code'
        };

        // MongoDB
        const mongoStart5 = Date.now();
        const agg = await collection.aggregate([
          { $group: { _id: '$city', avgScore: { $avg: '$score' }, count: { $sum: 1 } } }
        ]).toArray();
        response.mongodb = {
          success: true,
          result: agg,
          executionTime: Date.now() - mongoStart5
        };
        break;

    }
  } catch (error: any) {
    console.error('Query error:', error);
  } finally {
    await redisClient.quit();
    await mongoClient.close();
  }

  return response;
}

export async function executeCustomQuery(filter: any, sort: any, limit: number | null): Promise<QueryResult> {
  const { collection, mongoClient } = await getClients();
  
  try {
    const startTime = Date.now();
    
    let query = collection.find(filter);
    
    if (sort !== null && sort !== undefined) {
      query = query.sort(sort);
    }
    
    if (limit !== null && limit !== undefined && limit > 0) {
      query = query.limit(limit);
    }
    
    const result = await query.toArray();
    
    await mongoClient.close();
    
    return {
      success: true,
      result,
      executionTime: Date.now() - startTime
    };
  } catch (error: any) {
    await mongoClient.close();
    return {
      success: false,
      error: error.message
    };
  }
}
