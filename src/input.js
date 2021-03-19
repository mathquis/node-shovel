const Path       = require('path')
const Prometheus = require('prom-client')
const Node       = require('./node')
const NoopCodec  = require('./codec')
const Message    = require('./message')

class InputNode extends Node {
  constructor(pipelineConfig) {
    const {codec = {}, options = {}} = pipelineConfig.input

    super(pipelineConfig, options)

    this.codec = NoopCodec
    if ( codec.use ) {
      this.codec = this.pipelineConfig.loadFn(codec.use)(codec.options)
    }

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
    this.counter.inc({...this.defaultLabels, kind: 'error'})
    super.error(err)
  }

  up() {
    this.status.set({...this.defaultLabels, kind: 'up'}, 1)
    super.up()
  }

  down() {
    this.status.set({...this.defaultLabels, kind: 'up'}, 0)
    super.down()
  }

  async in(message) {
    this.counter.inc({...this.defaultLabels, kind: 'in'})
  }

  out(message) {
    this.log.debug('-> OUT %s', message)
    this.emit('out', message)
  }

  createMessage(content) {
  	return new Message(content)
  }

  ack(message) {
    this.counter.inc({...this.defaultLabels, kind: 'out'})
    this.counter.inc({...this.defaultLabels, kind: 'acked'})
    super.ack(message)
  }

  nack(message) {
    this.counter.inc({...this.defaultLabels, kind: 'out'})
    this.counter.inc({...this.defaultLabels, kind: 'nacked'})
    super.nack(message)
  }

  ignore(message) {
    this.counter.inc({...this.defaultLabels, kind: 'out'})
    this.counter.inc({...this.defaultLabels, kind: 'ignored'})
    super.ignore(message)
  }

  reject(message) {
    this.counter.inc({...this.defaultLabels, kind: 'out'})
    this.counter.inc({...this.defaultLabels, kind: 'rejected'})
    super.reject(message)
  }
}

module.exports = InputNode