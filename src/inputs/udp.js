const Dgram = require('dgram')

const META_UDP_PROPERTIES = 'input_udp_properties'

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
            default: 514
         },
         type: {
            doc: '',
            format: ['udp4', 'udp6'],
            default: 'udp4'
         }
      })
      .on('start', async () => {
         const {interface, port, type} = node.getConfig()

         server = Dgram.createSocket({type, reuseAddr: true})

         server
            .on('listening', () => {
               node.up()
            })
            .on('error', err => {
               node.error(err)
            })
            .on('message', (data, rinfo) => {
               node.in()
               try {
                  const messages = await node.decode(data)
                  messages.forEach(message => {
                     message.setMetas([
                        [META_UDP_PROPERTIES, rinfo]
                     ])
                     node.out(message)
                  })
               } catch (err) {
                  node.error(err)
                  node.reject()
               }
            })
            .bind({
               address: interface,
               port,
               exclusive: true
            })
      })
      .on('stop', async () => {
         if ( server ) {
            server.close()
         }
      })
}