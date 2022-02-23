const Path       = require('path')
const Prometheus = require('prom-client')
const Node       = require('./node')
const NoopCodec  = require('./codecs/noop')
const Utils      = require('./utils')

class OutputNode extends Node {
  constructor(pipelineConfig) {
    const {codec = {use: 'json'}, options = {}} = pipelineConfig.output

    super(pipelineConfig, options)

    let codecClass = NoopCodec
    if ( codec.use ) {
      try {
        codecClass = Utils.loadFn(codec.use, [Path.resolve(__dirname, './codecs'), pipelineConfig.path])
      } catch (err) {
        throw new Error(`Unknown codec "${codec.use} (${err.message})`)
      }
      this.log.info('Using codec: %s', codec.use)
    }
    this.codec = new codecClass(pipelineConfig, codec.options)

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