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

    this.inputStatus = new Prometheus.Gauge({
      name: 'input_status',
      help: 'Status of the input node',
      labelNames
    })

    this.inputMessage = new Prometheus.Counter({
      name: 'input_message',
      help: 'Number of input messages',
      labelNames
    })

    this.pipelineMessage = new Prometheus.Counter({
      name: 'pipeline_message',
      help: 'Number of pipeline messages',
      labelNames
    })

    this.outputStatus = new Prometheus.Gauge({
      name: 'output_status',
      help: 'Status of the output node',
      labelNames
    })

    this.outputMessage = new Prometheus.Counter({
      name: 'output_message',
      help: 'Number of output messages',
      labelNames
    })

    this.outputFlush = new Prometheus.Counter({
      name: 'output_flush',
      help: 'Number of output flushes',
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
      this.input = new inputClass(use, codecFn, options)
    } catch (err) {
      throw new Error(`Input error: ${err.message}`)
    }

    this.input
      .on('error', err => {
        this.inputMessage.inc({pipeline: this.name, kind: 'error'})
      })
      .on('up', () => {
        this.inputStatus.set({pipeline: this.name, kind: 'up'}, 1)
      })
      .on('down', () => {
        this.inputStatus.set({pipeline: this.name, kind: 'up'}, 0)
      })
      .on('in', () => {
        this.processingMessage.inc({pipeline: this.name})
        this.inputMessage.inc({pipeline: this.name, kind: 'received'})
      })
      .on('out', message => {
        this.pipeline.in(message)
        this.inputMessage.inc({pipeline: this.name, kind: 'emitted'})
      })
      .on('ack', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.inputMessage.inc({pipeline: this.name, kind: 'acked'})
        this.globalMessage.inc({pipeline: this.name})
      })
      .on('nack', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.inputMessage.inc({pipeline: this.name, kind: 'nacked'})
        this.globalMessage.inc({pipeline: this.name})
      })
      .on('reject', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.inputMessage.inc({pipeline: this.name, kind: 'rejected'})
        this.globalMessage.inc({pipeline: this.name})
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
        this.pipelineMessage.inc({pipeline: this.name, kind: 'error'})
      })
      .on('in', message => {
        this.pipelineMessage.inc({pipeline: this.name, kind: 'received'})
      })
      .on('out', message => {
        this.output.in(message)
        this.pipelineMessage.inc({pipeline: this.name, kind: 'emitted'})
      })
      .on('ack', message => {
        this.pipelineMessage.inc({pipeline: this.name, kind: 'acked'})
      })
      .on('nack', message => {
        this.input.nack(message)
        this.pipelineMessage.inc({pipeline: this.name, kind: 'nacked'})
      })
      .on('ignore', message => {
        this.input.ack(message)
        this.pipelineMessage.inc({pipeline: this.name, kind: 'ignored'})
      })
      .on('reject', message => {
        this.input.reject(message)
        this.pipelineMessage.inc({pipeline: this.name, kind: 'rejected'})
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
      this.output = new outputClass(use, codecFn, options)
    } catch (err) {
      throw new Error(`Output error: ${err.message}`)
    }

    this.output
      .on('error', err => {
        this.outputMessage.inc({pipeline: this.name, kind: 'error'})
      })
      .on('up', () => {
        this.outputStatus.set({pipeline: this.name, kind: 'up'}, 1)
      })
      .on('down', () => {
        this.outputStatus.set({pipeline: this.name, kind: 'up'}, 0)
      })
      .on('in', message => {
        this.outputMessage.inc({pipeline: this.name, kind: 'received'})
      })
      .on('out', message => {
        this.outputMessage.inc({pipeline: this.name, kind: 'emitted'})
      })
      .on('ack', message => {
        this.input.ack(message)
        this.outputMessage.inc({pipeline: this.name, kind: 'acked'})
      })
      .on('nack', message => {
        this.input.nack(message)
        this.outputMessage.inc({pipeline: this.name, kind: 'nacked'})
      })
      .on('reject', message => {
        this.input.reject(message)
        this.outputMessage.inc({pipeline: this.name, kind: 'rejected'})
      })
      .on('flush', () => {
        this.outputFlush.inc({pipeline: this.name})
      })
  }
}

module.exports = Processor