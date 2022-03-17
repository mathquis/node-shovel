export default node => {
   node
      .on('in', async (message) => {
         console.log(message)
         node.ack(message)
      })
}