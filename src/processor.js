const Path       = require('path')
const Prometheus = require('prom-client')
const Convict    = require('convict')
const Logger     = require('./logger')
const Utils      = require('./utils')
const Input      = require('./input')
const Pipeline   = require('./pipeline')
const Output     = require('./output')

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
         pipeline: this.pipeline.help(),
         output: this.output.help()
      }
   }

   async start() {
      await this.output.start()
      await this.pipeline.start()
      await this.input.start()
   }

   async stop() {
      await this.input.stop()
      await this.pipeline.start()
      await this.output.stop()
   }

   async setupInput() {
      try {
      this.input = new Input(this.pipelineConfig)
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
      } catch (err) {
         console.error(err)
      }
   }

   setupPipeline() {
      this.pipeline = new Pipeline(this.pipelineConfig)
      this.pipeline
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('out', message => {
            this.output.in(message)
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

   setupOutput() {
      this.output = new Output(this.pipelineConfig)
      this.output
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('ack', message => {
            this.pipeline.ack(message)
         })
         .on('nack', message => {
            this.pipeline.nack(message)
         })
         .on('ignore', message => {
            this.pipeline.ignore(message)
         })
         .on('reject', message => {
            this.pipeline.reject(message)
         })
   }
}

module.exports = Processor