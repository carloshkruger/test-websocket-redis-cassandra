import { setTimeout as sleep } from 'node:timers/promises'
import { WebSocket } from 'ws';

function getIntegerInRange(min, max) {  
  return Math.floor(
    Math.random() * (max - min) + min
  )
}

const NUMBER_OF_WS_CLIENTS = 1000
const NUMBER_OF_MESSAGES_SENT_BY_WS_CLIENT = 1000

for (let i = 1; i <= NUMBER_OF_WS_CLIENTS; i++) {
  const wsClient = new WebSocket('ws://localhost:300'+getIntegerInRange(0,2), 'ws', {
    headers: {
      userId: i
    }
  })

  wsClient.on('open', async () => {
    for (let j = 0; j < NUMBER_OF_MESSAGES_SENT_BY_WS_CLIENT; j++) {
      // according to my tests, 2 cassandra nodes can handle roughly 8000 requests per second
      // so we have 1000 clients and all of then send messages at the same time
      // which means, if each client sent 8 messages, it would be 8000 messages at the same time,
      // that is why we sleep for a second
      if (j % 8 === 0) {
        await sleep(1000)
      }
      const recipientId = getIntegerInRange(1, NUMBER_OF_WS_CLIENTS+1)
      wsClient.send(JSON.stringify({
        recipientId,
        content: `test message from user ${i} to user ${recipientId}`
      }))
    }
  })
}