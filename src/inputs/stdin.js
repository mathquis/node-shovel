const Readline = require('readline')
const InputNode = require('../input')

class StdinOutput extends InputNode {

  get configSchema() {
    return {}
  }

  async start() {
    this.reader = Readline.createInterface({
      input: process.stdin,
      terminal: false
    })

    this.reader.on('line', line => {
        this.log.debug('Received line:', line)
        const msg = this.createMessage(line)
        this.out(msg)
    })
    await super.start()
  }

  async stop() {
    // if ( this.reader ) {
    //   await this.reader.close()
    // }
    this.down()
    await super.stop()
  }
}

module.exports = StdinOutput