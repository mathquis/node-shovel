const AMQP      = require('amqplib')
const InputNode = require('../input')

const META_AMQP_FIELDS     = 'input_amqp_fields'
const META_AMQP_PROPERTIES = 'input_amqp_properties'

let consumers = 0

class AmqpInput extends InputNode {
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
      queue_name: {
        doc: '',
        default: 'indexer',
        arg: 'amqp-queue-name'
      },
      routing_key: {
        doc: '',
        default: ''
      },
      queue_size: {
        doc: '',
        default: 1000,
        arg: 'amqp-queue-size'
      },
      reconnect_after_ms: {
        doc: '',
        default: 5000,
        arg: 'amqp-reconnect-after-ms'
      }
    }
  }

  get consumerTag() {
    return 'shovel-amqp-input-' + consumers
  }

  get queueSize() {
    const queueSize = parseInt(this.getConfig('queue_size'))
    if ( !queueSize || isNaN(queueSize) ) {
      return 500
    }
    return queueSize
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

    await this.channel.prefetch(this.queueSize)

    const queue = await this.channel.assertQueue(this.getConfig('queue_name'), {
      durable: true
    })

    await this.channel.bindQueue(this.getConfig('queue_name'), this.getConfig('exchange_name'), this.getConfig('routing_key'))

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

    this.channel.consume(this.getConfig('queue_name'), async data => {
      this.in('[AMQP]')
      let message
      try {
        message = await this.decode(data)
        message.setMetas([
          [META_AMQP_FIELDS, data.fields],
          [META_AMQP_PROPERTIES, data.properties]
        ])
        this.out(message)
      } catch (err) {
        message = this.createMessage(data.content)
        message.setMetas([
          [META_AMQP_FIELDS, data.fields],
          [META_AMQP_PROPERTIES, data.properties]
        ])
        this.error(err)
        this.reject(message)
      }
    }, {
      consumerTag: this.consumerTag,
      noAck: false
    })

    this.up()
  }

  async start() {
    await this.connect()
    await super.start()
  }

  async stop() {
    if ( this.channel ) {
      await this.channel.cancel(this.consumerTag)
      await this.connection.close()
    }
    this.down()
    await super.stop()
  }

  ack(message) {
    if ( !this.channel ) return
    const fields = message.getMeta(META_AMQP_FIELDS)
    if ( fields ) {
      this.channel.ack({fields})
      super.ack(message)
    } else {
      this.error(new Error(`Unable to ack message (id: ${message.id})`))
    }
  }

  nack(message) {
    if ( !this.channel ) return
    const fields = message.getMeta(META_AMQP_FIELDS)
    if ( fields ) {
      this.channel.nack({fields}, false, true)
      super.nack(message)
    } else {
      this.error(new Error(`Unable to nack message (id: ${message.id})`))
    }
  }

  reject(message) {
    if ( !this.channel ) return
    const fields = message.getMeta(META_AMQP_FIELDS)
    if ( fields ) {
      this.channel.nack({fields}, false, false)
      super.reject(message)
    } else {
      this.error(new Error(`Unable to reject message (id: ${message.id})`))
    }
  }
}

module.exports = AmqpInput