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
         process.stdout.write(message.uuid + ' (' + message.getMeta('queue_retries') + ') -> ' + message.payload + '\n')
         node.ack(message)
      })
}