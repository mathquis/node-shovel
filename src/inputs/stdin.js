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
            node.in(line)
         })

         node.up()
      })
      .on('stop', async () => {
         process.stdin.unref()
      })
}