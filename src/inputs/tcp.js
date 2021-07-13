const Net       = require('net')
const Readline  = require('readline')
const InputNode = require('../input')

const META_TCP_PROPERTIES = 'input_tcp_properties'

class TcpInput extends InputNode {
  get configSchema() {
    return {
      interface: {
        doc: '',
        default: '',
        arg: 'tcp-interface',
        env: 'TCP_INTERFACE'
      },
      port: {
        doc: '',
        format: 'port',
        default: 514,
        arg: 'tcp-port',
        env: 'TCP_PORT'
      },
      'encoding': {
        doc: '',
        format: String,
        default: 'utf8'
      },
      'keep_alive': {
        'enabled': {
          doc: '',
           format: Boolean,
           default: true
        },
        'delay': {
           doc: '',
           format: Number,
           default: 30000
        }
      }
    }
  }

  async onMessage(data, rinfo) {
    this.in('[TCP]')
    let message
    try {
      message = await this.decode(data)
      message.setMetas([
        [META_TCP_PROPERTIES, rinfo]
      ])
      this.out(message)
    } catch (err) {
      this.error(err)
    }
  }

  async start() {
    this.server = Net.createServer(socket => {

      const {
        remoteAddress,
        remoteFamily,
        remotePort,
        localAddress,
        localPort
      } = socket

      this.log.debug('Incoming TCP connection from %s', remoteAddress)

      const encoding = this.getConfig('encoding')
      this.log.debug('Using encoding: %s', encoding)
      socket.setEncoding(this.getConfig('encoding'))

      const keepAliveEnabled = this.getConfig('keep_alive.enabled')
      const keepAliveDelay = this.getConfig('keep_alive.delay')
      this.log.debug('Using KeepAlive: %s (delay: %dms)', keepAliveEnabled, keepAliveDelay)
      socket.setKeepAlive(keepAliveEnabled, keepAliveDelay)

      const reader = Readline.createInterface({
        input: socket,
        terminal: false
      })

      reader.on('line', line => {
          this.onMessage(line, {
            remoteAddress,
            remoteFamily,
            remotePort,
            localAddress,
            localPort})
      })

      socket.on('error', err => {
        this.error(err)
        socket.end()
      })

      socket.on('end', () => {
        this.log.debug('TCP connection close')
      })
    })

    this.server.listen(
      this.getConfig('port'),
      this.getConfig('interface'),
      () => {
        this.up()
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

module.exports = TcpInput