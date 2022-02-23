const OutputNode = require('../output')

class StdoutOutput extends OutputNode {
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
    const data = await this.encode(message)
    process.stdout.write(data + '\n')
    this.ack(message)
  }
}

module.exports = StdoutOutput