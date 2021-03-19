const Prometheus = require('prom-client')
const Node       = require('./node')
const NoopCodec  = require('./codec')
const Message    = require('./message')

class InputNode extends Node {
  constructor(name, codec, options) {
    super(name, options)
    this.codec = codec || NoopCodec

    this.status = new Prometheus.Gauge({
      name: 'input_status',
      help: 'Status of the input node',
      labelNames: ['pipeline', 'kind']
    })

    this.counter = new Prometheus.Counter({
      name: 'input_message',
      help: 'Number of input messages',
      labelNames: ['pipeline', 'kind']
    })
  }

  async decode(data) {
    const content = await this.codec.decode(data)
    return this.createMessage(content)
  }

  error(err) {
    this.counter.inc({pipeline: this.name, kind: 'error'})
    super.error(err)
  }

  up() {
    this.status.set({pipeline: this.name, kind: 'up'}, 1)
    super.up()
  }

  down() {
    this.status.set({pipeline: this.name, kind: 'up'}, 0)
    super.down()
  }

  async in(message) {
    this.counter.inc({pipeline: this.name, kind: 'in'})
  }

  out(message) {
    this.log.debug('-> OUT %s', message)
    this.emit('out', message)
  }

  createMessage(content) {
  	return new Message(content)
  }

  ack(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'acked'})
    super.ack(message)
  }

  nack(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'nacked'})
    super.nack(message)
  }

  ignore(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'ignored'})
    super.ignore(message)
  }

  reject(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'rejected'})
    super.reject(message)
  }
}

module.exports = InputNode