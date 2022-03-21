import AMQP from 'amqplib'

const META_AMQP_PUBLISH_OPTIONS = 'output-amqp-publish-options'
const META_AMQP_ROUTING_KEY     = 'output-amqp-routing-key'

let consumers = 0

export default node => {
   let connection, channel, reconnectTimeout

   consumers++
   const consumerTag = 'shovel-amqp-output-' + consumers

   node
      .registerConfig({
         host: {
            doc: '',
            format: String,
            default: 'localhost'
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
         routing_key: {
            doc: '',
            format: String,
            default: ''
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
      .onStart(async () => {
         connect()
      })
      .onStop(async () => {
         if ( channel ) {
            await channel.close()
         }
      })
      .onIn(async (message) => {
         if ( !channel || !node.isUp || node.isPaused ) {
            node.nack(message)
            return
         }
         const {
            exchange_name: exchangeName,
            routing_key: routingKey
         } = node.getConfig()

         const routingKeyTemplate = message.getHeader(META_AMQP_ROUTING_KEY) || routingKey
         const finalRoutingKey = node.util.renderTemplate(routingKeyTemplate, message)
         node.log.debug('Publishing message with routing key "%s"', finalRoutingKey)

         await channel.publish(exchangeName, finalRoutingKey, Buffer.from(message.payload), message.getHeader(META_AMQP_PUBLISH_OPTIONS) || {})

         node.ack(message)
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

      const {reconnect_after_ms: reconnectAfterMs} = node.getConfig()

      channel = await connection.createChannel()

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

      node.up()
   }
}