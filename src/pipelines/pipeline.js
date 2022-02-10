const Path       = require('path')
const Prometheus = require('prom-client')
const Node       = require('../node')
const Utils      = require('../utils')

class Pipeline extends Node {
  constructor(pipelineConfig) {
    const {use, options} = pipelineConfig.pipeline

    super(pipelineConfig, options)

    this.fn = (message, next) => {
      next(null, [message])
    }

    if ( use ) {
      this.fn = Utils.loadFn(use, [pipelineConfig.path])(this, options)
    }

    this.counter = new Prometheus.Counter({
      name: 'pipeline_message',
      help: 'Number of input messages',
      labelNames: ['pipeline', 'kind']
    })
  }

  error(err) {
    this.counter.inc({...this.defaultLabels, kind: 'error'})
    super.error(err)
  }

  up() {
    // Nothing
  }

  down() {
    // Nothing
  }

  async in(message) {
    this.counter.inc({...this.defaultLabels, kind: 'in'})
    await super.in(message)
    try {
      await new Promise(async (resolve, reject) => {
        try {
          await this.fn(message, (err, messages) => {
            if ( !messages ) {
              if ( err ) {
                this.error(err)
                this.reject(message)
              } else {
                this.ignore(message)
              }
            } else {
              messages.forEach(message => {
                this.ack(message)
              })
            }
            resolve()
          })
        } catch (err) {
          reject(err)
        }
      })
    } catch (err) {
      this.error(err)
      this.nack(message)
    }
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

module.exports = Pipeline