const Readline = require('readline')

module.exports = node => {
   let reader

   node
      .on('start', async () => {
         reader = Readline.createInterface({
            input: process.stdin,
            terminal: false
         })

         reader.on('line', async line => {
            node.log.debug('Received line: %s (length: %d)', line, line.length)
            if ( line.length === 0 ) return
            node.in(line)
         })

         node.up()
      })
      .on('stop', async () => {
         process.stdin.unref()
      })
}