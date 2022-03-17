import AMQP from 'amqplib'

const META_AMQP_FIELDS     = 'input-amqp-fields'
const META_AMQP_PROPERTIES = 'input-amqp-properties'

let consumers = 0

export default node => {
   let connection, channel, consuming, reconnectTimeout

   consumers++
   const consumerTag = 'shovel-amqp-input-' + consumers

   node
      .registerConfig({
         host: {
            doc: '',
            format: String,
            default: 'localhost',
         },
         port: {
            doc: '',
            format: 'port',
            default: 5672
         },
         vhost: {
            doc: '',
            format: String,
            default: '/'
         },
         username: {
            doc: '',
            format: String,
            default: ''
         },
         password: {
            doc: '',
            format: String,
            default: '',
            sensitive: true
         },
         exchange_name: {
            doc: '',
            format: String,
            default: ''
         },
         queue_name: {
            doc: '',
            format: String,
            default: ''
         },
         routing_key: {
            doc: '',
            format: String,
            default: ''
         },
         queue_size: {
            doc: '',
            format: Number,
            default: 1000
         },
         durable: {
            doc: '',
            format: Boolean,
            default: true
         },
         auto_delete: {
            doc: '',
            format: Boolean,
            default: true
         },
         heartbeat: {
            doc: '',
            format: 'duration',
            default: '60s'
         },
         reconnect_after_ms: {
            doc: '',
            format: 'duration',
            default: '5s'
         }
      })
      .on('start', async () => {
         connect()
      })
      .on('stop', async () => {
         if ( channel ) {
            await channel.cancel(consumerTag)
            await connection.close()
         }
      })
      .on('up', async () => {
         startConsuming()
      })
      .on('pause', async () => {
         stopConsuming()
      })
      .on('resume', async () => {
         startConsuming()
      })
      .on('ack', async (message) => {
         ackMessage(message)
      })
      .on('nack', async (message) => {
         nackMessage(message, true)
      })
      .on('ignore', async (message) => {
         ackMessage(message)
      })
      .on('reject', async (message) => {
         nackMessage(message, false)
      })

   async function connect() {
      try {
         node.log.debug('Connecting...')

         clearReconnectTimeout()

         // Connect to AMQP
         const {host: hostname, port, vhost, username, password, heartbeat} = node.getConfig()

         connection = await AMQP.connect({
            hostname,
            port,
            vhost,
            username,
            password,
            heartbeat
         })

         connection
            .on('close', err => {
               node.log.debug('Connection closed')
               if ( err ) {
                  reconnect(err)
               }
            })
            .on('block', (reason) => {
               node.log.warn('Connection blocked by server (reason: %s)', reason)
               node.down()
            })
            .on('unblock', () => {
               node.up()
            })
            .on('error', err => {
               reconnect(err)
            })

         await onConnect()
      } catch (err) {
         reconnect(err)
      }
   }

   async function reconnect(err) {
      if ( err ) {
         node.error(err)
      }
      const reconnectAfterMs = node.getConfig('reconnect_after_ms')
      node.log.debug('Reconnecting in %d...', reconnectAfterMs)
      if ( connection ) {
         connection.removeAllListeners()
         connection = null
      }
      if ( channel ) {
         channel.removeAllListeners()
         channel = null
      }
      node.down()
      clearReconnectTimeout()
      reconnectTimeout = setTimeout(() => {
         connect()
      }, reconnectAfterMs)
   }

   function clearReconnectTimeout() {
      if ( !reconnectTimeout ) return
      clearTimeout(reconnectTimeout)
      reconnectTimeout = null
   }

   async function onConnect() {
      node.log.debug('Connected')

      await createChannel()
      await startConsuming()

      node.up()
   }

   async function createChannel() {
      const {
         queue_name: queueName,
         queue_size: queueSize,
         durable,
         auto_delete: autoDelete,
         exchange_name: exchangeName,
         routing_key: routingKey,
         reconnect_after_ms: reconnectAfterMs
      } = node.getConfig()

      channel = await connection.createChannel()

      await channel.prefetch(queueSize)

      const queue = await channel.assertQueue(queueName, {
         durable,
         autoDelete
      })

      await channel.bindQueue(queueName, exchangeName, routingKey)

      channel
         .on('close', () => {
            node.log.debug('Channel closed')
            channel = null
         })
         .on('error', err => {
            node.error(err)
            channel = null
            setTimeout(() => {
               onConnect()
            }, reconnectAfterMs)
         })
   }

   async function startConsuming() {
      if ( !channel ) {
         return
      }
      if ( consuming ) {
         return
      }
      consuming = true
      const {
         queue_name: queueName,
      } = node.getConfig()
      await channel.consume(queueName, async data => {
         if ( !data ) {
            return
         }
         const message = createMessage(data)
         node.in(message)
      }, {
         consumerTag,
         noAck: false
      })
   }

   async function stopConsuming() {
      if ( !channel ) {
         return
      }
      if ( !consuming ) {
         return
      }
      consuming = false
      await channel.cancel(consumerTag)
   }

   function createMessage(data) {
      const message = node.createMessage()
      message.source = data.content
      if ( data.properties.contentType ) {
         message.setContentType(data.properties.contentType)
      }
      message.setHeaders({
         [META_AMQP_FIELDS]: data.fields,
         [META_AMQP_PROPERTIES]: data.properties
      })
      return message
   }

   function ackMessage(message) {
      if ( !channel ) {
         return
      }
      const fields = message.getHeader(META_AMQP_FIELDS)
      if ( fields ) {
         channel.ack({fields})
      } else {
         node.error(new Error(`Unable to ack message (id: ${message.id})`))
      }
   }

   function nackMessage(message, requeue) {
      if ( !channel ) {
         return
      }
      const fields = message.getHeader(META_AMQP_FIELDS)
      if ( fields ) {
         channel.nack({fields}, false, !!requeue)
      } else {
         node.error(new Error(`Unable to reject message (id: ${message.id})`))
      }
   }
}