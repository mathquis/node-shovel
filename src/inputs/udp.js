const Dgram     = require('dgram')
const InputNode = require('../input')

const META_UDP_PROPERTIES = 'input_udp_properties'

class UdpInput extends InputNode {
  get configSchema() {
    return {
      interface: {
        doc: '',
        default: '',
        arg: 'udp-interface',
        env: 'UDP_INTERFACE'
      },
      port: {
        doc: '',
        format: 'port',
        default: 514,
        arg: 'udp-port',
        env: 'UDP_PORT'
      }
    }
  }

  async onMessage(data, rinfo) {
    this.in('[UDP]')
    let message
    try {
      message = await this.decode(data)
      message.setMetas([
        [META_UDP_PROPERTIES, rinfo]
      ])
      this.out(message)
    } catch (err) {
      this.error(err)
    }
  }

  async start() {
    this.server = Dgram.createSocket({type: 'udp4', reuseAddr: true})
    this.server
      .on('listening', () => {
        this.up()
      })
      .on('error', err => {
        this.error(err)
      })
      .on('message', (data, rinfo) => {
        this.onMessage(data, rinfo)
      })
      .bind(
        {
          address: this.getConfig('interface'),
          port: this.getConfig('port'),
          exclusive: true
        }
      )
    await super.start()
  }

  async stop() {
    if ( this.server ) {
      this.server.close()
    }
    this.down()
    await super.stop()
  }
}

module.exports = UdpInput