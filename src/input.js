const Node		= require('./node')
const NoopCodec	= require('./codec')
const Message 	= require('./message')

class InputNode extends Node {
  constructor(name, codec, options) {
    super(name, options)
    this.codec = codec || NoopCodec
  }

  async decode(data) {
    const content = await this.codec.decode(data)
    return this.createMessage(content)
  }

  createMessage(content) {
  	return new Message(content)
  }
}

module.exports = InputNode