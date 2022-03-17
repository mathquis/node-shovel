export default node => {
   node
      .registerConfig({})
      .on('in', async (message) => {
         process.stdout.write(message.payload + '\n')
         node.ack(message)
      })
}