const Dgram = require('dgram')

module.exports = node => {
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
         }
      })
      .on('start', () => {
         client = Dgram.createSocket('udp4')
      })
      .on('in', async (message) => {
         const content = await node.encode(message)
         if ( !content ) return
         client.send(content + '\n', node.getConfig('port'), node.getConfig('host'), err => {
            if ( err ) {
               node.nack(message)
               node.error(err)
               return
            }
            node.ack(message)
         })
      })
}