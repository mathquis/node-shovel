const AMQP       = require('amqplib')
const Compile    = require('string-template/compile')
const OutputNode = require('../output')

const AMQP_OUTPUT_OPTIONS = 'amqp_publish_options'
const AMQP_OUTPUT_ROUTING_KEY = 'amqp_routing_key'
let consumers = 0

class AmqpOutput extends OutputNode {
  constructor(name, codec, options) {
    super(name, codec, options)
    this.routingKeyTemplates = new Map()
    this.compileRoutingKeyTemplate(this.getConfig('routing_key'))
  }

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
        .on('close', () => {
          this.log.warn('Connection closed')
          this.reconnect()
        })
        .on('error', err => {
          this.emit('error', err)
          this.reconnect()
        })

      await this.onConnect()
    } catch (err) {
      this.emit('error', err)
      this.reconnect()
    }
  }

  async reconnect() {
    this.log.debug('Reconnecting in %d...', this.reconnectAfterMs)
    this.connection = null
    this.channel = null
    this.emit('down')
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
        this.log.info('Channel closed')
        this.channel = null
      })
      .on('error', err => {
        this.emit('error', err)
        this.channel = null
        setTimeout(() => {
          this.onConnect()
        }, this.reconnectAfterMs)
      })

    this.flush()

    this.emit('up')
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

  compileRoutingKeyTemplate(template) {
    this.routingKeyTemplates.set(template, Compile(template))
  }

  formatRoutingKey(message) {
    let routingKey = message.getMeta(AMQP_OUTPUT_ROUTING_KEY)
    if ( !routingKey ) {
      routingKey = this.getConfig('routing_key')
    }
    let routingKeyTemplate = this.routingKeyTemplates.get(routingKey)
    if ( !routingKeyTemplate ) {
      routingKeyTemplate = Compile(routingKey)
      this.routingKeyTemplates.set(routingKey, routingKeyTemplate)
    }
    const {date} = message
    return routingKeyTemplate({
      ...message.content,
      YYYY: date.getFullYear(),
      YY: date.getYear(),
      MM: date.getUTCMonth().toString().padStart(2, '0'),
      M: date.getUTCMonth(),
      DD: date.getUTCDate().toString().padStart(2, '0'),
      D: date.getUTCDate()
    })
  }

  async write(message) {
    await super.write(message)
    try {
      if ( this.channel ) {
        const routingKey = this.formatRoutingKey(message)
        const content = await this.encode(message)
        await this.channel.publish(this.getConfig('exchange_name'), routingKey, content, message.getMeta(AMQP_OUTPUT_OPTIONS) || {})
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

  async flush() {
      // Get the queue messages
      const messages = this.queue || []
      this.queue = []
      await messages.reduce(async (p, message) => {
        await p
        return this.write(message)
      }, Promise.resolve())
  }
}

module.exports = AmqpOutput