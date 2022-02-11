const Path          = require('path')
const Prometheus    = require('prom-client')
const Convict       = require('convict')
const Logger        = require('./logger')
const Utils         = require('./utils')
const Pipeline      = require('./pipelines/pipeline')

const registers = []

class Processor {
  constructor(pipelineConfig) {
    this.pipelineConfig = pipelineConfig

    this.log = Logger.child({category: this.pipelineConfig.name})

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

    this.setupInput()
    this.setupPipeline()
    this.setupOutput()
  }

  get defaultLabels() {
    return {pipeline: this.pipelineConfig.name}
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

  async setupInput() {
    const {use = '', options = {}} = this.pipelineConfig.input

    let inputClass
    try {
      inputClass = Utils.loadFn(use, [Path.resolve(__dirname, './inputs'), this.pipelineConfig.path])
    } catch (err) {
      this.log.error(err.stack)
      throw new Error(`Unknown input type "${use} (${err.message})`)
    }

    try {
      this.input = new inputClass(this.pipelineConfig)
    } catch (err) {
      throw new Error(`Input error: ${err.message}`)
    }

    this.input
      .on('error', err => {
        this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
      })
      .on('in', () => {
        this.globalMessage.inc({...this.defaultLabels, kind:'in'})
      })
      .on('out', message => {
        this.processingMessage.inc({pipeline: this.name})
        this.pipeline.in(message)
      })
      .on('ack', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({...this.defaultLabels, kind: 'out'})
        this.globalMessage.inc({...this.defaultLabels, kind: 'acked'})
      })
      .on('nack', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({...this.defaultLabels, kind: 'out'})
        this.globalMessage.inc({...this.defaultLabels, kind: 'nacked'})
      })
      .on('ignore', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({...this.defaultLabels, kind: 'out'})
        this.globalMessage.inc({...this.defaultLabels, kind: 'ignored'})
      })
      .on('reject', message => {
        this.processingMessage.dec({pipeline: this.name})
        this.globalMessage.inc({...this.defaultLabels, kind: 'out'})
        this.globalMessage.inc({...this.defaultLabels, kind: 'rejected'})
      })
  }

  setupPipeline() {
    this.pipeline = new Pipeline(this.pipelineConfig)
    this.pipeline
      .on('error', err => {
        this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
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

  setupOutput() {
    const {use = '', options = {}} = this.pipelineConfig.output
    let outputClass
    try {
      outputClass = Utils.loadFn(use, [Path.resolve(__dirname, './outputs'), this.pipelineConfig.path])
    } catch (err) {
      throw new Error(`Unknown output type "${use} (${err.message})`)
    }
    try {
      this.output = new outputClass(this.pipelineConfig)
    } catch (err) {
      throw new Error(`Output error: ${err.message}`)
    }

    this.output
      .on('error', err => {
        this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
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