import Net from 'net'

export default node => {
   let client, reconnectTimeout

   node
      .registerConfig({
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
      })
      .on('start', async () => {
         connect()
      })
      .on('stop', async () => {
         if ( client ) {
            await new Promise((resolve, reject) => {
               client.removeAllListeners()
               client.on('close', () => {
                  client = null
                  resolve()
               })
               client.end()
            })
         }
      })
      .on('in', async (message) => {
         if ( !client || client.pending || !node.isUp || node.isPaused ) {
            node.nack(message)
            return
         }
         try {
            client.write(message.payload, 'utf8', err => {
               if ( err ) {
                  queue.push(message)
                  return
               }
               node.ack(message)
            })
         } catch (err) {
            node.reject(message)
            node.error(err)
         }
      })

   async function connect() {
      try {
         node.log.debug('Connecting...')

         clearReconnectTimeout()

         client = new Net.Socket()

         const {host, port} = node.getConfig()

         client.connect(
            port,
            host,
            () => {
               node.log.debug('Connected')
               node.up()
            }
         )

         client
            .on('close', err => {
               node.log.debug('Connection closed')
               if ( err ) {
                  node.error(err)
               }
               reconnect()
            })
            .on('error', err => {
               node.error(err)
               reconnect()
            })

      } catch (err) {
         node.error(err)
         reconnect()
      }
   }

   async function reconnect() {
      const reconnectAfterMs = node.config.get('reconnect_after_ms')
      node.log.debug('Reconnecting in %d...', reconnectAfterMs)
      client.removeAllListeners();
      client = null
      node.down()
      clearReconnectTimeout()
      reconnectTimeout = setTimeout(() => {
         connect()
      }, reconnectAfterMs)
   }

   function clearReconnectTimeout() {
      if ( !reconnectTimeout ) return
      clearTimeout(reconnectTimeout)
      reconnectTimeout = null
   }

   function onConnect() {
      node.log.debug('Connected')
      node.up()
   }
}