import { MongoClient } from 'mongodb';
import WebSocket from 'ws';

const MONGO_URL = 'mongodb://mongo1:27017,mongo2:27017,mongo3:27017,mongo4:27017,mongo5:27017,mongo6:27017,mongo7:27017/?replicaSet=rs0';

export class WriteConcerns {
  private clients: Set<WebSocket> = new Set();

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

  async testWriteConcern(concernType: 'w1' | 'w3' | 'w5' | 'majority') {
    let mongoClient: MongoClient | null = null;

    try {
      mongoClient = new MongoClient(MONGO_URL, {
        serverSelectionTimeoutMS: 120000,
        connectTimeoutMS: 120000
      });
      await mongoClient.connect();

      const db = mongoClient.db('writeconcern_test');
      const collection = db.collection('documents');

      // Determine write concern configuration
      let writeConcern: any;
      let concernLabel: string;
      let acknowledgedNodes: number[];

      switch (concernType) {
        case 'w1':
          writeConcern = { w: 1 };
          concernLabel = 'w:1 (Primary Only)';
          acknowledgedNodes = [1]; // Only primary
          break;
        case 'w3':
          writeConcern = { w: 3 };
          concernLabel = 'w:3 (Primary + 2 Secondaries)';
          acknowledgedNodes = [1, 2, 3]; // Primary + two secondaries
          break;
        case 'w5':
          writeConcern = { w: 5 };
          concernLabel = 'w:5 (Primary + 4 Secondaries)';
          acknowledgedNodes = [1, 2, 3, 4, 5]; // Primary + four secondaries
          break;
        case 'majority':
          writeConcern = { w: 'majority' };
          concernLabel = 'w:"majority" (5 of 9 nodes)';
          acknowledgedNodes = [1, 2, 3, 4, 5]; // Majority = 5 nodes
          break;
      }

      // Measure write time
      const startTime = Date.now();
      
      const documentId = Date.now();
      await collection.insertOne(
        {
          _id: documentId as any,
          timestamp: new Date(),
          concernType,
          data: 'Test write with different concerns'
        },
        { writeConcern }
      );

      const executionTime = Date.now() - startTime;

      // Broadcast result
      this.broadcast({
        type: 'write-concern-result',
        concernType: writeConcern.w,
        concernLabel,
        executionTime,
        acknowledgedNodes,
        documentId
      });

      await mongoClient.close();
    } catch (error) {
      console.error('Write concern test error:', error);
      if (mongoClient) {
        await mongoClient.close();
      }
      throw error;
    }
  }
}
