import { setTimeout as sleep } from 'node:timers/promises'
import { WebSocket } from 'ws';

function getIntegerInRange(min, max) {  
  return Math.floor(
    Math.random() * (max - min) + min
  )
}

const NUMBER_OF_WS_CLIENTS = 10_000
const NUMBER_OF_MESSAGES_SENT_PER_WS_CLIENT = 10

for (let i = 1; i <= NUMBER_OF_WS_CLIENTS; i++) {
  const wsClient = new WebSocket('ws://localhost:300'+getIntegerInRange(0,2), 'ws', {
    headers: {
      userId: i
    }
  })

  wsClient.on('open', async () => {
    for (let j = 0; j < NUMBER_OF_MESSAGES_SENT_PER_WS_CLIENT; j++) {
      // With 10k users sending messages at the same time, Cassandra will not be able to handle
      // this quantity of requests (it is possible to add more nodes to the cluster) thats why we sleep for 1 sec
      await sleep(1000)
      const recipientId = getIntegerInRange(1, NUMBER_OF_WS_CLIENTS+1)
      wsClient.send(JSON.stringify({
        recipientId,
        content: `test message from user ${i} to user ${recipientId}`
      }))
    }
  })
}