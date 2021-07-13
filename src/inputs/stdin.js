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

    this.reader.on('line', async line => {
        this.log.debug('Received line: %s', line)
        this.in('[STDIN]')
        const message = await this.decode(line)
        this.out(message)
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