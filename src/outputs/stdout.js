module.exports = node => {
   node
      .on('in', async (message) => {
         if ( !message.payload ) return
         process.stdout.write(message.payload + '\n')
         node.ack(message)
      })
}