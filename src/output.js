const Prometheus = require('prom-client')
const Node       = require('./node')
const NoopCodec  = require('./codec')

class OutputNode extends Node {
  constructor(name, codec, options) {
    super(name, options)
    this.codec = codec || NoopCodec

    this.status = new Prometheus.Gauge({
      name: 'output_status',
      help: 'Status of the output node',
      labelNames: ['pipeline', 'kind']
    })

    this.counter = new Prometheus.Counter({
      name: 'output_message',
      help: 'Number of output messages',
      labelNames: ['pipeline', 'kind']
    })
  }

  async encode(message) {
    return await this.codec.encode(message)
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

module.exports = OutputNode