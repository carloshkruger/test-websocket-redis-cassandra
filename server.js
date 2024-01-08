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

const redisClient = createRedisClient(REDIS_SERVER)
redisClient.on('error', err => console.log('Redis Client Error', err))

const REDIS_NOTIFICATIONS_TOPIC_PREFIX = 'app:notifications'

const redisSubscriber = redisClient.duplicate();
await redisSubscriber.connect();

const redisPublisher = redisClient.duplicate();
await redisPublisher.connect();

const WEB_SOCKET_PORT = Number(process.env.WEB_SOCKET_PORT || 3000);
const wsServer = new WebSocketServer({ port : WEB_SOCKET_PORT });
 
wsServer.on('connection', async function connection(ws, req) {
  const userId = Number(req.headers.userid)
  const topicName = `${REDIS_NOTIFICATIONS_TOPIC_PREFIX}:${userId}`

  console.log('new connection', userId) 

  await redisSubscriber.subscribe(topicName, message => {
    console.log('message:', message)
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });

  ws.on('close', async () => {
    console.log('connection closed', userId)
    await redisSubscriber.unsubscribe(topicName)
  })

  ws.on('error', async () => {
    console.log('some error happened')
    await redisSubscriber.unsubscribe(topicName)
  })

  ws.on('message', async message => {
    const data = JSON.parse(message)
    const recipientId = Number(data.recipientId)

    await cassandraClient.execute('INSERT INTO messages (id, authorId, recipientId, content, createdAt) VALUES (?, ?, ?, ?, ?)', [
      randomUUID(),
      userId,
      recipientId,
      data.content,
      new Date()
    ], {
      prepare: true
    })

    const recipientTopic = `${REDIS_NOTIFICATIONS_TOPIC_PREFIX}:${recipientId}`
    await redisPublisher.publish(recipientTopic, data.content)
  })
});