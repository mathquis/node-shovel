export default node => {
   node
      .onIn(async (message) => {
         node.ack(message)
      })
}