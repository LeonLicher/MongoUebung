import { Collection, MongoClient } from 'mongodb';
import { createClient, RedisClientType } from 'redis';
import WebSocket from 'ws';

const MONGO_URL = 'mongodb://mongo1:27017,mongo2:27017,mongo3:27017,mongo4:27017,mongo5:27017,mongo6:27017,mongo7:27017/?replicaSet=rs0';
const REDIS_URL = 'redis://redis:6379';
const LEASE_DURATION = 10000; // 10 seconds
const HEARTBEAT_INTERVAL = 3000; // 3 seconds

interface LeaderDocument {
  _id: string;
  nodeId: string;
  expiresAt: number;
  updatedAt: number;
}

interface Node {
  id: string;
  status: 'idle' | 'competing' | 'leader' | 'follower' | 'crashed';
  lease: number | null;
  heartbeatTimer: NodeJS.Timeout | null;
  competitionTimer: NodeJS.Timeout | null;
}

export class LeaderElection {
  private nodes: Map<string, Node> = new Map();
  private currentDatabase: 'mongodb' | 'redis' = 'mongodb';
  private mongoClient: MongoClient | null = null;
  private redisClient: RedisClientType | null = null;
  private leaderCollection: Collection<LeaderDocument> | null = null;
  private clients: Set<WebSocket> = new Set();
  private running = false;

  constructor() {
    // Initialize 5 nodes
    for (let i = 1; i <= 5; i++) {
      const nodeId = `node-${i}`;
      this.nodes.set(nodeId, {
        id: nodeId,
        status: 'idle',
        lease: null,
        heartbeatTimer: null,
        competitionTimer: null
      });
    }
  }

  addClient(ws: WebSocket) {
    this.clients.add(ws);
  }

  removeClient(ws: WebSocket) {
    this.clients.delete(ws);
  }

  broadcast(data: any) {
    const message = JSON.stringify(data);
    this.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  }

  async startElection(database: 'mongodb' | 'redis') {
    if (this.running) {
      this.stopElection();
    }

    this.currentDatabase = database;
    this.running = true;

    // Initialize database connection
    if (database === 'mongodb') {
      await this.initMongo();
    } else {
      await this.initRedis();
    }

    this.broadcast({ type: 'election-started', database });

    // Start all nodes competing
    for (const [nodeId, node] of this.nodes) {
      if (node.status !== 'crashed') {
        this.startNodeCompetition(nodeId);
      }
    }
  }

  async stopElection() {
    this.running = false;

    // Clear all timers
    for (const node of this.nodes.values()) {
      if (node.heartbeatTimer) clearInterval(node.heartbeatTimer);
      if (node.competitionTimer) clearTimeout(node.competitionTimer);
      node.heartbeatTimer = null;
      node.competitionTimer = null;
    }

    // Close connections
    if (this.mongoClient) {
      await this.mongoClient.close();
      this.mongoClient = null;
      this.leaderCollection = null;
    }
    if (this.redisClient) {
      await this.redisClient.quit();
      this.redisClient = null;
    }
  }

  async resetElection() {
    await this.stopElection();

    // Reset all nodes
    for (const node of this.nodes.values()) {
      node.status = 'idle';
      node.lease = null;
    }

    this.broadcast({ type: 'election-reset' });
  }

  async crashNode(nodeId: string) {
    const node = this.nodes.get(nodeId);
    if (!node || node.status === 'crashed') return;

    const wasLeader = node.status === 'leader';

    // Clear timers
    if (node.heartbeatTimer) clearInterval(node.heartbeatTimer);
    if (node.competitionTimer) clearTimeout(node.competitionTimer);
    node.heartbeatTimer = null;
    node.competitionTimer = null;

    node.status = 'crashed';
    node.lease = null;

    this.broadcast({ type: 'node-crashed', nodeId });

    // If leader crashed, release the lock
    if (wasLeader) {
      if (this.currentDatabase === 'mongodb') {
        await this.releaseMongoLock();
      } else {
        await this.releaseRedisLock();
      }
      this.broadcast({ type: 'leader-lost', nodeId });
    }
  }

  private async initMongo() {
    this.mongoClient = new MongoClient(MONGO_URL, {
      serverSelectionTimeoutMS: 120000,
      connectTimeoutMS: 120000
    });
    await this.mongoClient.connect();
    const db = this.mongoClient.db('leaderelection');
    this.leaderCollection = db.collection('leader');
    
    // Clear any existing leader
    await this.leaderCollection.deleteMany({});
  }

  private async initRedis() {
    this.redisClient = createClient({ url: REDIS_URL }) as RedisClientType;
    await this.redisClient.connect();
    
    // Clear any existing leader key
    await this.redisClient.del('leader');
  }

  private async startNodeCompetition(nodeId: string) {
    const node = this.nodes.get(nodeId);
    if (!node || node.status === 'crashed') return;

    node.status = 'competing';
    this.broadcast({ 
      type: 'node-update', 
      nodeId, 
      status: 'competing',
      lease: null 
    });

    // Random delay before attempting to acquire leadership (0-2 seconds)
    const delay = Math.random() * 2000;
    
    node.competitionTimer = setTimeout(async () => {
      if (!this.running || node.status === 'crashed') return;

      const acquired = this.currentDatabase === 'mongodb' 
        ? await this.tryAcquireMongoLeadership(nodeId)
        : await this.tryAcquireRedisLeadership(nodeId);

      if (acquired) {
        node.status = 'leader';
        node.lease = Date.now() + LEASE_DURATION;
        
        this.broadcast({ 
          type: 'node-update', 
          nodeId, 
          status: 'leader',
          lease: node.lease 
        });
        this.broadcast({ type: 'leader-elected', nodeId });

        // Start heartbeat to maintain leadership
        this.startHeartbeat(nodeId);
      } else {
        node.status = 'follower';
        this.broadcast({ 
          type: 'node-update', 
          nodeId, 
          status: 'follower',
          lease: null 
        });

        // Retry after a delay
        node.competitionTimer = setTimeout(() => {
          if (this.running && node.status !== 'crashed') {
            this.startNodeCompetition(nodeId);
          }
        }, 5000);
      }
    }, delay);
  }

  private async tryAcquireMongoLeadership(nodeId: string): Promise<boolean> {
    if (!this.leaderCollection) return false;

    try {
      const now = Date.now();
      const expiresAt = now + LEASE_DURATION;

      // Try to acquire leadership only if no valid leader exists
      const result = await this.leaderCollection.findOneAndUpdate(
        { 
          _id: 'current-leader' as any,
          $or: [
            { expiresAt: { $lt: now } }, // Lease expired
            { expiresAt: { $exists: false } } // No leader
          ]
        },
        { 
          $set: { 
            nodeId, 
            expiresAt,
            updatedAt: now
          } 
        },
        { returnDocument: 'after' }
      );

      // If no document was found, try to insert a new one
      if (!result?.value) {
        try {
          await this.leaderCollection.insertOne({
            _id: 'current-leader' as any,
            nodeId,
            expiresAt,
            updatedAt: now
          });
          return true;
        } catch (insertError: any) {
          // If duplicate key error, someone else became leader
          if (insertError.code === 11000) {
            return false;
          }
          throw insertError;
        }
      }

      return result.value.nodeId === nodeId;
    } catch (error) {
      console.error('MongoDB leadership acquisition error:', error);
      return false;
    }
  }

  private async tryAcquireRedisLeadership(nodeId: string): Promise<boolean> {
    if (!this.redisClient) return false;

    try {
      // Try to set the leader key with NX (only if not exists) and PX (expiration in ms)
      const result = await this.redisClient.set('leader', nodeId, {
        NX: true,
        PX: LEASE_DURATION
      });

      return result === 'OK';
    } catch (error) {
      console.error('Redis leadership acquisition error:', error);
      return false;
    }
  }

  private startHeartbeat(nodeId: string) {
    const node = this.nodes.get(nodeId);
    if (!node) return;

    node.heartbeatTimer = setInterval(async () => {
      if (!this.running || node.status === 'crashed' || node.status !== 'leader') {
        if (node.heartbeatTimer) clearInterval(node.heartbeatTimer);
        return;
      }

      const renewed = this.currentDatabase === 'mongodb'
        ? await this.renewMongoLease(nodeId)
        : await this.renewRedisLease(nodeId);

      if (renewed) {
        node.lease = Date.now() + LEASE_DURATION;
        this.broadcast({ 
          type: 'node-update', 
          nodeId, 
          status: 'leader',
          lease: node.lease 
        });
      } else {
        // Lost leadership
        node.status = 'follower';
        node.lease = null;
        if (node.heartbeatTimer) clearInterval(node.heartbeatTimer);
        node.heartbeatTimer = null;
        
        this.broadcast({ type: 'leader-lost', nodeId });
        this.broadcast({ 
          type: 'node-update', 
          nodeId, 
          status: 'follower',
          lease: null 
        });

        // Try to compete again
        this.startNodeCompetition(nodeId);
      }
    }, HEARTBEAT_INTERVAL);
  }

  private async renewMongoLease(nodeId: string): Promise<boolean> {
    if (!this.leaderCollection) return false;

    try {
      const now = Date.now();
      const expiresAt = now + LEASE_DURATION;

      const result = await this.leaderCollection.findOneAndUpdate(
        { _id: 'current-leader' as any, nodeId },
        { $set: { expiresAt, updatedAt: now } },
        { returnDocument: 'after' }
      );

      return result?.value?.nodeId === nodeId;
    } catch (error) {
      console.error('MongoDB lease renewal error:', error);
      return false;
    }
  }

  private async renewRedisLease(nodeId: string): Promise<boolean> {
    if (!this.redisClient) return false;

    try {
      const currentLeader = await this.redisClient.get('leader');
      
      if (currentLeader === nodeId) {
        await this.redisClient.pExpire('leader', LEASE_DURATION);
        return true;
      }
      
      return false;
    } catch (error) {
      console.error('Redis lease renewal error:', error);
      return false;
    }
  }

  private async releaseMongoLock() {
    if (!this.leaderCollection) return;
    try {
      await this.leaderCollection.deleteOne({ _id: 'current-leader' as any });
    } catch (error) {
      console.error('MongoDB lock release error:', error);
    }
  }

  private async releaseRedisLock() {
    if (!this.redisClient) return;
    try {
      await this.redisClient.del('leader');
    } catch (error) {
      console.error('Redis lock release error:', error);
    }
  }
}
