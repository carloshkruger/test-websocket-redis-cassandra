import { randomUUID } from 'node:crypto'
import WebSocket, { WebSocketServer } from 'ws';
import { createClient as createRedisClient } from 'redis'
import { Client as CassandraClient } from 'cassandra-driver'

// Inspect the Cassandra Docker containers to get the IPAddress
// docker inspect cassandra
// docker inspect cassandra2
const cassandraClient = new CassandraClient({
  contactPoints: ['172.18.0.2:9042', '172.18.0.3:9042'],
  localDataCenter: 'datacenter1',
  keyspace: 'messages',
  credentials: {
    username: 'cassandra',
    password: 'cassandra'
  },
})
await cassandraClient.connect()

const REDIS_SERVER = "redis://localhost:6379";
const WEB_SOCKET_PORT = Number(process.env.WEB_SOCKET_PORT || 3000);

const redisClient = createRedisClient(REDIS_SERVER)
redisClient.on('error', err => console.log('Redis Client Error', err))

const REDIS_NOTIFICATIONS_TOPIC = 'app:notifications'

const redisSubscriber = redisClient.duplicate();
await redisSubscriber.connect();

const redisPublisher = redisClient.duplicate();
await redisPublisher.connect();

const wsConnections = new Map()

await redisSubscriber.subscribe(REDIS_NOTIFICATIONS_TOPIC, message => {
  const data = JSON.parse(message)
  const wsClient = wsConnections.get(Number(data.userId))

  if (wsClient && wsClient.readyState === WebSocket.OPEN) {
    console.log(data.content)
    wsClient.send(data.content);
  }
});

const wsServer = new WebSocketServer({ port : WEB_SOCKET_PORT });
 
wsServer.on('connection', function connection(ws, req) {
  const userId = Number(req.headers.userid)
  wsConnections.set(userId, ws)

  ws.on('close', () => {
    console.log('connection closed', userId)
    wsConnections.delete(userId)
  })

  ws.on('error', () => {
    console.log('some error happened')
    wsConnections.delete(userId)
  })

  ws.on('message', async message => {
    const data = JSON.parse(message)
    await cassandraClient.execute('INSERT INTO messages (id, authorId, recipientId, content, createdAt) VALUES (?, ?, ?, ?, ?)', [
      randomUUID(),
      userId,
      Number(data.recipientId),
      data.content,
      new Date()
    ], {
      prepare: true
    })
    await redisPublisher.publish(REDIS_NOTIFICATIONS_TOPIC, JSON.stringify({
      userId: data.recipientId,
      content: data.content
    }))
  })

  console.log('new connection', userId) 
});