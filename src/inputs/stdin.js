const Readline = require('readline')
const InputNode = require('../input')

class StdinInput extends InputNode {

  get configSchema() {
    return {}
  }

  async start() {
    this.reader = Readline.createInterface({
      input: process.stdin,
      terminal: false
    })

    this.reader.on('line', line => {
        this.log.debug('Received line: %s', line)
        const msg = this.createMessage(line)
        msg.setId(msg.uuid)
        this.out(msg)
    })

    await super.start()
    this.up()
  }

  async stop() {
    process.stdin.unref()
    this.down()
    await super.stop()
  }
}

module.exports = StdinInput