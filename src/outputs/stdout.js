module.exports = node => {
   node
      .on('in', async (message) => {
         const content = await node.encode(message)
         if ( !content ) return
         process.stdout.write(content + '\n')
         node.ack(message)
      })
      .on('start', async () => {
         await node.up()
      })
}