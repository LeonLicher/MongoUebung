import express, { Request, Response } from 'express';
import http from 'http';
import path from 'path';
import WebSocket from 'ws';
import { LeaderElection } from './leaderElection';
import { runPerformanceBenchmark } from './performanceBenchmark';
import { executeCustomQuery, executeQuery, initializeData, insertDocument } from './queryCapabilities';
import { WriteConcerns } from './writeConcerns';

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const PORT = 3000;

const leaderElection = new LeaderElection();
const writeConcerns = new WriteConcerns();

app.use(express.json());
app.use(express.static(path.join(__dirname, '../src/public')));

app.get('/api/benchmark', async (req: Request, res: Response) => {
  try {
    const results = await runPerformanceBenchmark();
    res.json(results);
  } catch (error) {
    const err = error as Error;
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/init-data', async (req: Request, res: Response) => {
  try {
    const result = await initializeData();
    res.json(result);
  } catch (error) {
    const err = error as Error;
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/query/:type', async (req: Request, res: Response) => {
  try {
    const result = await executeQuery(req.params.type);
    res.json(result);
  } catch (error) {
    const err = error as Error;
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/custom-query', async (req: Request, res: Response) => {
  try {
    const { filter, sort, limit } = req.body;
    const result = await executeCustomQuery(filter, sort, limit);
    res.json(result);
  } catch (error) {
    const err = error as Error;
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/insert-document', async (req: Request, res: Response) => {
  try {
    const { document } = req.body;
    const result = await insertDocument(document);
    res.json(result);
  } catch (error) {
    const err = error as Error;
    res.status(500).json({ error: err.message });
  }
});

// WebSocket handling
wss.on('connection', (ws: WebSocket) => {
  console.log('WebSocket client connected');
  leaderElection.addClient(ws);
  writeConcerns.addClient(ws);

  ws.on('message', async (message: string) => {
    try {
      const data = JSON.parse(message.toString());
      
      switch (data.action) {
        case 'start-election':
          await leaderElection.startElection(data.database);
          break;
        case 'reset-election':
          await leaderElection.resetElection();
          break;
        case 'crash-node':
          await leaderElection.crashNode(data.nodeId);
          break;
        case 'test-write-concern':
          await writeConcerns.testWriteConcern(data.concernType);
          break;
      }
    } catch (error) {
      console.error('WebSocket message error:', error);
    }
  });

  ws.on('close', () => {
    console.log('WebSocket client disconnected');
    leaderElection.removeClient(ws);
    writeConcerns.removeClient(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
