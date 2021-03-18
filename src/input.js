const Node = require('./node')

class InputNode extends Node {
  constructor(name, parser, options) {
    super(name, options)
    this.parser = parser || ( content => content )
  }

  async parse(content) {
    return await this.parser(content)
  }
}

module.exports = InputNode