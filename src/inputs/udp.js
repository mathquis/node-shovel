import Dgram from 'dgram'

const META_UDP_PROPERTIES = 'input-udp-properties'

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
            default: 514
         },
         type: {
            doc: '',
            format: ['udp4', 'udp6'],
            default: 'udp4'
         }
      })
      .onStart(async () => {
         const {host, port, type} = node.getConfig()

         server = Dgram.createSocket({type, reuseAddr: true})

         server
            .on('listening', () => {
               node.log.info('Listening (host: %s, port: %d, type: %s)', host, port, type)
               node.up()
            })
            .on('error', err => {
               node.error(err)
            })
            .on('message', (data, rinfo) => {
               if ( !listening ) {
                  return
               }

               const message = node.createMessage()

               message.source = data

               message.setHeaders({
                  [META_UDP_PROPERTIES]: rinfo
               })

               node.in(message)
            })
            .bind({
               address: host,
               port,
               exclusive: true
            })

         node.log.info('Listening (host: %s, port: %d, type: %s)', host, port, type)
      })
      .onStop(async () => {
         if ( server ) {
            server.close()
         }
      })
      .onUp(async () => {
         listening = true
      })
      .onPause(async () => {
         listening = false
      })
      .onResume(async () => {
         listening = true
      })
}