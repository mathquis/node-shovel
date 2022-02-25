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
            default: 'exchange'
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

      const {queue_name, queue_size, exchange_name, routing_key, reconnect_after_ms} = node.getConfig()

      channel = await connection.createChannel()

      await channel.prefetch(queue_size)

      const queue = await channel.assertQueue(queue_name, {
         durable: true
      })

      await channel.bindQueue(queue_name, exchange_name, routing_key)

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
            }, reconnect_after_ms)
         })

      channel.consume(queue_name, async data => {
         node.in()
         let message
         try {
            messages = await node.decode(data.content)
            messages.forEach(message => {
               message.setMetas([
                  [META_AMQP_FIELDS, data.fields],
                  [META_AMQP_PROPERTIES, data.properties]
               ])
               node.out(message)
            })
         } catch (err) {
            message = node.createMessage(data.content)
            message.setMetas([
               [META_AMQP_FIELDS, data.fields],
               [META_AMQP_PROPERTIES, data.properties]
            ])
            node.error(err)
            node.reject(message)
         }
      }, {
         consumerTag,
         noAck: false
      })

      node.up()
   }


}