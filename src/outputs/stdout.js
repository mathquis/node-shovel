module.exports = node => {
   node
      .registerConfig({
         debug: {
            doc: '',
            format: Boolean,
            default: false
         }
      })
      .on('in', async (message) => {
         if ( node.getConfig('debug') ) {
            console.log(message)
         }
         process.stdout.write(message.payload + '\n')
         node.ack(message)
      })
}