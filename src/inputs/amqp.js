const AMQP = require('amqplib')

const META_AMQP_FIELDS     = 'input_amqp_fields'
const META_AMQP_PROPERTIES = 'input_amqp_properties'

let consumers = 0

module.exports = node => {
   let connection, channel, reconnectTimeout

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
         reconnect_after_ms: {
            doc: '',
            format: Number,
            default: 5000
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
      .on('ack', (message) => {
         if ( !channel ) return
         const fields = message.getMeta(META_AMQP_FIELDS)
         if ( fields ) {
            channel.ack({fields})
         } else {
            node.error(new Error(`Unable to ack message (id: ${message.id})`))
         }
      })
      .on('nack', (message) => {
         if ( !channel ) return
         const fields = message.getMeta(META_AMQP_FIELDS)
         if ( fields ) {
            channel.nack({fields}, false, true)
         } else {
            node.error(new Error(`Unable to nack message (id: ${message.id})`))
         }
      })
      .on('ignore', (message) => {
         if ( !channel ) return
         const fields = message.getMeta(META_AMQP_FIELDS)
         if ( fields ) {
            channel.ack({fields})
         } else {
            node.error(new Error(`Unable to ignore message (id: ${message.id})`))
         }
      })
      .on('reject', (message) => {
         if ( !channel ) return
         const fields = message.getMeta(META_AMQP_FIELDS)
         if ( fields ) {
            channel.nack({fields}, false, false)
         } else {
            node.error(new Error(`Unable to reject message (id: ${message.id})`))
         }
      })

   async function connect() {
      try {
         node.log.debug('Connecting...')

         clearReconnectTimeout()

         // Connect to AMQP
         const {host: hostname, port, vhost, username, password} = node.getConfig()

         connection = await AMQP.connect({
            hostname,
            port,
            vhost,
            username,
            password
         })

         connection
            .on('close', err => {
               node.log.debug('Connection closed')
               if ( err ) {
                  reconnect(err)
               }
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
      connection = null
      channel = null
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

      channel.consume(queueName, async data => {
         const options = {
            contentType: data.properties.contentType,
            metas: [
               [META_AMQP_FIELDS, data.fields],
               [META_AMQP_PROPERTIES, data.properties]
            ]
         }
         node.in(data.content, options)
      }, {
         consumerTag,
         noAck: false
      })

      node.up()
   }


}