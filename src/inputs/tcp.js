const Net       = require('net')
const Readline  = require('readline')

const META_TCP_PROPERTIES = 'input_tcp_properties'

module.exports = node => {
   let server

   node
      .registerConfig({
         interface: {
            doc: '',
            format: String,
            default: ''
         },
         port: {
            doc: '',
            format: 'port',
            default: 514,
         },
         encoding: {
            doc: '',
            format: String,
            default: 'utf8'
         },
         keep_alive: {
            enabled: {
               doc: '',
                format: Boolean,
                default: true
            },
            delay: {
                doc: '',
                format: Number,
                default: 30000
            }
         }
      })
      .on('start', async () => {
         server = Net.createServer(socket => {
            const {
               remoteAddress,
               remoteFamily,
               remotePort,
               localAddress,
               localPort
            } = socket

            node.log.debug('Incoming TCP connection from %s', remoteAddress)

            const {encoding, keep_alive, interface, port} = node.getConfig()

            node.log.debug('Using encoding: %s', encoding)
            socket.setEncoding(encoding)

            const keepAliveEnabled = keep_alive.enabled
            const keepAliveDelay = keep_alive.delay
            node.log.debug('Using KeepAlive: %s (delay: %dms)', keepAliveEnabled, keepAliveDelay)
            socket.setKeepAlive(keepAliveEnabled, keepAliveDelay)

            const reader = Readline.createInterface({
               input: socket,
               terminal: false
            })

            reader.on('line', line => {
               node.in()
               try {
                  const messages = await node.decode(line)
                  messages.forEach(message => {
                     message.setMetas([
                        [META_TCP_PROPERTIES, {
                           remoteAddress,
                           remoteFamily,
                           remotePort,
                           localAddress,
                           localPort
                        }]
                     ])
                     node.out(message)
                  })
               } catch (err) {
                  node.error(err)
                  node.reject()
               }
            })

            socket.on('error', err => {
               node.error(err)
               socket.end()
            })

            socket.on('end', () => {
               node.log.debug('TCP connection close')
            })
         })

         server.listen(
            port,
            interface,
            () => {
               node.up()
            }
         )
      })
      .on('stop', async () => {
         if ( server ) {
            server.close()
         }
      })
}