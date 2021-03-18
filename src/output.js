const Node		= require('./node')
const NoopCodec	= require('./codec')

class OutputNode extends Node {
  constructor(name, codec, options) {
    super(name, options)
    this.codec = codec || NoopCodec
  }

  async encode(message) {
    return await this.codec.encode(message)
  }

  async write(message) {
    this.emit('incoming', message)
  }

  async flush() {
    this.emit('flush')
  }
}

module.exports = OutputNode