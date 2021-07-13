const Dgram      = require('dgram')
const OutputNode = require('../output')

class UdpOutput extends OutputNode {
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
      }
    }
  }

  async setup() {
    this.client = Dgram.createSocket('udp4')
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
    await super.start()
    this.up()
  }

  async stop() {
    this.log.debug('Stopping...')
    this.down()
    await super.stop()
  }

  async in(message) {
    await super.in(message)
    const data = await this.encode(message)
    this.client.send(data + '\n', this.getConfig('port'), this.getConfig('host'), err => {
      if ( err ) {
        this.nack(message)
        this.error(err)
        return
      }
      this.ack(message)
    })
  }
}

module.exports = UdpOutput