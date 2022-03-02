module.exports = node => {
   node
      .on('in', async (message) => {
         node.ack(message)
      })
      .on('start', async () => {
         await node.up()
      })
}