const Node		= require('./node')
const NoopCodec	= require('./codec')

class InputNode extends Node {
  constructor(name, codec, options) {
    super(name, options)
    this.codec = codec || NoopCodec
  }

  async decode(content) {
    return await this.codec.decode(content)
  }
}

module.exports = InputNode