const Path       = require('path')
const Prometheus = require('prom-client')
const Node       = require('./node')
const NoopCodec  = require('./codec')

class OutputNode extends Node {
  constructor(pipelineConfig) {
    const {codec = {}, options = {}} = pipelineConfig.output

    super(pipelineConfig, options)

    this.codec = NoopCodec
    if ( codec.use ) {
      this.codec = this.pipelineConfig.loadFn(codec.use)(codec.options)
    }

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
    await super.in(message)
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

module.exports = OutputNode