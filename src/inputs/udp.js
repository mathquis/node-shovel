const AMQP      = require('amqplib')
const InputNode = require('../input')

let consumers = 0

class UdpInput extends InputNode {
  get configSchema() {
    return {
      interface: {
        doc: '',
        default: '0.0.0.0',
        arg: 'udp-interface',
        env: 'UDP_INTERFACE'
      },
      port: {
        doc: '',
        format: 'port',
        default: 5544,
        arg: 'udp-port',
        env: 'UDP_PORT'
      }
    }
  }

  async connect() {
    try {
      this.log.debug('Connecting...')
      await this.onConnect()
    } catch (err) {
      this.error(err)
      this.reconnect()
    }
  }

  async reconnect() {
    this.log.debug('Reconnecting in %d...', this.reconnectAfterMs)
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

  async start() {
    await this.connect()
    await super.start()
  }

  async stop() {
    this.down()
    await super.stop()
  }
}

module.exports = UdpInput