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

  async in(message) {
    process.stdout.write(JSON.stringify(message, null, this.getConfig('pretty') ? 2 : 0) + '\n')
  }
}

module.exports = StdoutOutput