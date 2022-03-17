export default node => {
   node.on('in', async (message) => {
      node.out(message)
   })
}