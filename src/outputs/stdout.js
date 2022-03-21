export default node => {
   node
      .registerConfig({})
      .onIn(async (message) => {
         process.stdout.write(message.payload + '\n')
         node.ack(message)
      })
}