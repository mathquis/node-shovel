import Net from 'net'
import Readline from 'readline'

const META_TCP_PROPERTIES = 'input-tcp-properties'

export default node => {
   let server, listening

   node
      .registerConfig({
         host: {
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

            const {encoding, keep_alive, host, port} = node.getConfig()

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
               if ( !listening ) {
                  return
               }

               const message = node.createMessage()

               message.source = line

               message.setHeaders({
                  [META_TCP_PROPERTIES]: {
                     remoteAddress,
                     remoteFamily,
                     remotePort,
                     localAddress,
                     localPort
                  }
               })

               node.in(message)
            })

            socket.on('error', err => {
               node.error(err)
               socket.end()
            })

            socket.on('end', () => {
               node.log.debug('TCP connection close')
            })
         })

         node.log.info('Listening (host: %s, port: %d)', host, port)

         server.listen(
            port,
            host,
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
      .on('up', async () => {
         listening = true
      })
      .on('pause', async () => {
         listening = false
      })
      .on('resume', async () => {
         listening = true
      })
}