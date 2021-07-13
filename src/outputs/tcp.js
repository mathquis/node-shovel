const Net        = require('net')
const OutputNode = require('../output')

class TcpOutput extends OutputNode {
  get configSchema() {
    return {
      host: {
        doc: '127.0.0.1',
        format: String,
        default: 'localhost'
      },
      port: {
        doc: '',
        format: 'port',
        default: 515
      },
      reconnect_after_ms: {
        doc: '',
        format: Number,
        default: 5000
      }
    }
  }

  get reconnectAfterMs() {
    const reconnectAfterMs = parseInt(this.getConfig('reconnect_after_ms'))
    if ( !reconnectAfterMs || isNaN(reconnectAfterMs) ) {
      return 500
    }
    return reconnectAfterMs
  }

  async setup() {
    this.queue = []
  }

  async connect() {
    try {
      this.log.debug('Connecting...')

      this.clearReconnectTimeout()

      this.client = new Net.Socket()

      this.client.connect(
        this.getConfig('port'),
        this.getConfig('host'),
        () => {
          this.onConnect()
        }
      )

      this.client
        .on('close', err => {
          this.log.debug('Connection closed')
          if ( err ) {
            this.error(err)
          }
          this.reconnect()
        })
        .on('error', err => {
          this.error(err)
          this.reconnect()
        })

    } catch (err) {
      this.error(err)
      this.reconnect()
    }
  }

  async reconnect() {
    this.log.debug('Reconnecting in %d...', this.reconnectAfterMs)
    this.client.removeAllListeners();
    this.client = null
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
    this.up()
  }

  up() {
    super.up()
    const queue = this.queue
    this.queue = []
    queue.forEach(message => {
      this.in(message)
    })
  }

  async start() {
    this.log.debug('Starting...')
    await this.connect()
    await super.start()
  }

  async stop() {
    this.log.debug('Stopping...')
    if ( this.client ) {
      this.client.end()
      this.client.removeAllListeners();
      this.client = null
    }
    this.down()
    await super.stop()
  }

  async in(message) {
    await super.in(message)
    try {
      if ( this.client && !this.client.pending ) {
        const data = await this.encode(message)
        this.client.write(data + '\n', 'utf8', err => {
          if ( err ) {
            this.queue.push(message)
            return
          }
          this.ack(message)
        })
      } else {
        this.queue.push(message)
      }
    } catch (err) {
      this.nack(message)
      this.error(err)
    }
  }
}

module.exports = TcpOutput