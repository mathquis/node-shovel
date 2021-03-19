const AMQP       = require('amqplib')
const OutputNode = require('../output')

const META_AMQP_PUBLISH_OPTIONS = 'output_amqp_publish_options'
const META_AMQP_ROUTING_KEY     = 'output_amqp_routing_key'

let consumers = 0

class AmqpOutput extends OutputNode {
  get configSchema() {
    return {
      host: {
        doc: '',
        default: 'localhost',
        arg: 'amqp-host',
        env: 'RABBITMQ_HOST'
      },
      port: {
        doc: '',
        format: 'port',
        default: 5672,
        arg: 'amqp-port',
        env: 'RABBITMQ_PORT'
      },
      vhost: {
        doc: '',
        default: '/',
        arg: 'amqp-vhost',
        env: 'RABBITMQ_VHOST'
      },
      username: {
        doc: '',
        default: '',
        env: 'RABBITMQ_USERNAME'
      },
      password: {
        doc: '',
        default: '',
        sensitive: true,
        env: 'RABBITMQ_PASSWORD'
      },
      exchange_name: {
        doc: '',
        default: 'exchange',
        arg: 'amqp-exchange-name'
      },
      routing_key: {
        doc: '',
        default: ''
      },
      reconnect_after_ms: {
        doc: '',
        default: 5000,
        arg: 'amqp-reconnect-after-ms'
      }
    }
  }

  get consumerTag() {
    return 'shovel-amqp-output-' + consumers
  }

  get reconnectAfterMs() {
    const reconnectAfterMs = parseInt(this.getConfig('reconnect_after_ms'))
    if ( !reconnectAfterMs || isNaN(reconnectAfterMs) ) {
      return 500
    }
    return reconnectAfterMs
  }

  async connect() {
    try {
      this.log.debug('Connecting...')

      this.clearReconnectTimeout()

      // Connect to AMQP
      const {host: hostname, port, vhost, username, password} = this

      const opts = {
        hostname  : this.getConfig('host'),
        port      : this.getConfig('port'),
        vhost     : this.getConfig('vhost'),
        username  : this.getConfig('username'),
        password  : this.getConfig('password')
      }

      this.connection = await AMQP.connect(opts)

      this.connection
        .on('close', err => {
          this.log.debug('Connection closed')
          if ( err ) {
            this.error(err)
            this.reconnect()
          }
        })
        .on('error', err => {
          this.error(err)
          this.reconnect()
        })

      await this.onConnect()
    } catch (err) {
      this.error(err)
      this.reconnect()
    }
  }

  async reconnect() {
    this.log.debug('Reconnecting in %d...', this.reconnectAfterMs)
    this.connection = null
    this.channel = null
    this.down()
    this.clearReconnectTimeout()
    this.reconnectTimeout = setTimeout(() => {
      this.connect()
    }, this.reconnectAfterMs)
  }

  clearReconnectTimeout() {
    if ( !this.reconnectTimeout ) return
    clearTimeout(this.reconnectTimeout)
    this.reconnectTimeout = null
  }

  async onConnect() {
    this.log.debug('Connected')

    this.channel = await this.connection.createChannel()

    this.channel
      .on('close', () => {
        this.log.debug('Channel closed')
        this.channel = null
      })
      .on('error', err => {
        this.error(err)
        this.channel = null
        setTimeout(() => {
          this.onConnect()
        }, this.reconnectAfterMs)
      })

    // Get the queue messages
    const messages = this.queue || []
    this.queue = []
    await messages.reduce(async (p, message) => {
      await p
      return this.write(message)
    }, Promise.resolve())

    this.up()
  }

  async start() {
    this.log.debug('Starting...')
    await this.connect()
    await super.start()
  }

  async stop() {
    this.log.debug('Stopping...')
    if ( this.channel ) {
      await this.channel.close()
    }
    await super.stop()
  }

  async in(message) {
    await super.in(message)
    try {
      if ( this.channel ) {
        const routingKeyTemplate = message.getMeta(META_AMQP_ROUTING_KEY) || this.getConfig('routing_key')
        const routingKey = this.renderTemplate(routingKeyTemplate, message)
        this.log.debug('Publishing message with routing key "%s"', routingKey)
        const content = await this.encode(message)
        await this.channel.publish(this.getConfig('exchange_name'), routingKey, content, message.getMeta(META_AMQP_PUBLISH_OPTIONS) || {})
        this.ack(message)
        return
      } else {
        this.queue.push(message)
      }
    } catch (err) {
      this.nack(message)
      this.error(err)
    }
  }
}

module.exports = AmqpOutput