import Dgram from 'dgram'

export default node => {
   let client

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
         type: {
            doc: '',
            format: ['udp4', 'udp6'],
            default: 'udp4'
         }
      })
      .onStart(() => {
         const {type} = node.getConfig()
         client = Dgram.createSocket(type)
      })
      .onIn(async (message) => {
         const {
            host,
            port
         } = node.getConfig()
         client.send(message.payload, port, host, err => {
            if ( err ) {
               node.error(err)
               node.reject(message)
               return
            }
            node.ack(message)
         })
      })
}