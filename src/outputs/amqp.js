const AMQP       = require('amqplib')

const META_AMQP_PUBLISH_OPTIONS = 'output_amqp_publish_options'
const META_AMQP_ROUTING_KEY     = 'output_amqp_routing_key'

let consumers = 0

module.exports = node => {
  let connection, channel, reconnectTimeout

  let queue = []

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
        default: 'exchange'
      },
      routing_key: {
        doc: '',
        format: String,
        default: ''
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
        await channel.close()
      }
    })
    .on('up', () => {
      const q = queue || []
      queue = []
      q.forEach(node.in)
    })
    .on('in', async (message) => {
      try {
        if ( channel ) {
          const {exchange_name, routing_key} = node.getConfig()
          const routingKeyTemplate = message.getMeta(META_AMQP_ROUTING_KEY) || routing_key
          const routingKey = node.util.renderTemplate(routingKeyTemplate, message)
          node.log.debug('Publishing message with routing key "%s"', routingKey)
          const content = await node.encode(message)
          if (!content) return
          await channel.publish(exchange_name, routingKey, Buffer.from(content), message.getMeta(META_AMQP_PUBLISH_OPTIONS) || {})
          node.ack(message)
        } else {
          queue.push(message)
        }
      } catch (err) {
        node.nack(message)
        node.error(err)
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

    const {queue_size, reconnect_after_ms} = node.getConfig()

    channel = await connection.createChannel()

    await channel.prefetch(queue_size)

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

    node.up()
  }
}