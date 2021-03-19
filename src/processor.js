const Path          = require('path')
const Prometheus    = require('prom-client')
const Convict       = require('convict')
const Logger        = require('./logger')
const Pipeline      = require('./pipeline')

const registers = []

class Processor {
  constructor(name, options) {
    this.config = Convict({
      metrics: {
        labels: {
          doc: '',
          default: [],
          format: Array
        }
      }
    })
    this.config.load(options)
    this.config.validate({allowed: 'strict'})

    this.name = name
    this.log = Logger.child({category: this.name})
    this.setupMetrics(options)
  }

  help() {
    return {
      input: this.input.help(),
      output: this.output.help()
    }
  }

  async start() {
    await this.output.start()
    await this.input.start()
  }

  async stop() {
    await this.input.stop()
    await this.output.stop()
  }

  setupMetrics(options) {
    const labelNames = ['kind', 'pipeline']

    this.globalMessage = new Prometheus.Counter({
      name: 'message_processed',
      help: 'Number of messages',
      labelNames
    })

    this.processingMessage = new Prometheus.Gauge({
      name: 'message_processing',
      help: 'Number of messages currently in the processing pipeline',
      labelNames
    })
  }

  setupInput(input) {
    const {use = '', codec = {}, options = {}} = input || {}
    let inputClass
    try {
      inputClass = require('./inputs/' + use)
    } catch (err) {
      throw new Error(`Unknown input type "${use} (${err.message})`)
    }
    let codecFn
    if ( codec.use ) {
      try {
        codecFn = require(Path.resolve(process.cwd(), codec.use))(codec.options)
      } catch (err) {
        throw new Error(`Unknown codec "${codec.use} (${err.message})`)
      }
    }
    try {
      this.input = new inputClass(this.name, codecFn, options)
    } catch (err) {
      throw new Error(`Input error: ${err.message}`)
    }

    this.input
      .on('error', err => {
        this.globalMessage.inc({pipeline: this.name, kind: 'error'})
      })
      .on('in', () => {
        this.processingMessage.inc({pipeline: this.name})
        this.globalMessage.inc({pipeline: this.name, kind:'in'})
      })
      .on('out', message => {
        this.pipeline.in(message)
      })
      .on('ack', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({pipeline: this.name, kind: 'out'})
        this.globalMessage.inc({pipeline: this.name, kind: 'acked'})
      })
      .on('nack', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({pipeline: this.name, kind: 'out'})
        this.globalMessage.inc({pipeline: this.name, kind: 'nacked'})
      })
      .on('ignore', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({pipeline: this.name, kind: 'out'})
        this.globalMessage.inc({pipeline: this.name, kind: 'ignored'})
      })
      .on('reject', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({pipeline: this.name, kind: 'out'})
        this.globalMessage.inc({pipeline: this.name, kind: 'rejected'})
      })
  }

  setupPipeline(pipeline) {
    const {use = '', options = {}} = pipeline || {}
    let pipelineFn
    try {
      pipelineFn = require(Path.resolve(process.cwd(), use))(options)
    } catch (err) {
      throw new Error(`Unknown pipeline "${use} (${err.message})`)
    }

    this.pipeline = new Pipeline(this.name, pipelineFn)
    this.pipeline
      .on('error', err => {
        this.globalMessage.inc({pipeline: this.name, kind: 'error'})
      })
      .on('ack', message => {
        this.output.in(message)
      })
      .on('nack', message => {
        this.input.nack(message)
      })
      .on('ignore', message => {
        this.input.ignore(message)
      })
      .on('reject', message => {
        this.input.reject(message)
      })
  }

  setupOutput(output) {
    const {use = '', codec = {}, options = {}} = output || {}
    let outputClass
    try {
      outputClass = require('./outputs/' + use)
    } catch (err) {
      throw new Error(`Unknown output type "${use} (${err.message})`)
    }
    let codecFn
    if ( codec.use ) {
      try {
        codecFn = require(Path.resolve(process.cwd(), codec.use))(codec.options)
      } catch (err) {
        throw new Error(`Unknown codec "${codec.use} (${err.message})`)
      }
    }
    try {
      this.output = new outputClass(this.name, codecFn, options)
    } catch (err) {
      throw new Error(`Output error: ${err.message}`)
    }

    this.output
      .on('error', err => {
        this.globalMessage.inc({pipeline: this.name, kind: 'error'})
      })
      .on('ack', message => {
        this.input.ack(message)
      })
      .on('nack', message => {
        this.input.nack(message)
      })
      .on('ignore', message => {
        this.input.ignore(message)
      })
      .on('reject', message => {
        this.input.reject(message)
      })
  }
}

module.exports = Processor