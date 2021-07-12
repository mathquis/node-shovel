const OutputNode = require('../output')

class StdoutOutput extends OutputNode {

  get configSchema() {
    return {
      'pretty': {
        doc: '',
        default: true,
        format: Boolean,
        arg: 'stdout-pretty'
      }
    }
  }

  async start() {
    await super.start();
    this.up()
  }

  async stop() {
    this.down()
    await super.stop();
  }

  async in(message) {
    await super.in(message)
    process.stdout.write(JSON.stringify(message, null, this.getConfig('pretty') ? 2 : 0) + '\n')
    this.ack(message)
  }
}

module.exports = StdoutOutput