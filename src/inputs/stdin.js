import Readline from 'readline'

export default node => {
   let reader

   if ( node.pipelineConfig.workers > 1 ) {
      node.log.warn('Using this input with more than 1 worker is likely to cause issue. Use with caution.')
   }

   node
      .onStart(async () => {
         reader = Readline.createInterface({
            input: process.stdin,
            terminal: false
         })

         reader
            .on('line', async line => {
               node.log.debug('Received line: %s (length: %d)', line, line.length)
               const message = node.createMessage()
               message.source = line
               message.setContentType('text/plain')
               node.in(message)
            })
            .on('close', () => {
               node.shutdown()
            })

         node.up()
      })
      .onStop(async () => {
         if ( reader ) {
            reader.close()
         }
      })
      .onPause(async () => {
         if ( reader ) {
            reader.pause()
         }
      })
      .onResume(async () => {
         if ( reader ) {
            reader.resume()
         }
      })
}