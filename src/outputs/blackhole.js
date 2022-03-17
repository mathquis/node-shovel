export default node => {
   node
      .on('in', async (message) => {
         node.ack(message)
      })
}