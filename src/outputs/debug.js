export default node => {
   node
      .onIn(async (message) => {
         console.log(message)
         node.ack(message)
      })
}