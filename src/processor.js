const Path       = require('path')
const Prometheus = require('prom-client')
const Convict    = require('convict')
const Logger     = require('./logger')
const Utils      = require('./utils')
const Input      = require('./input')
const Decoder    = require('./decoder')
const Pipeline   = require('./pipeline')
const Encoder    = require('./encoder')
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
      this.setupDecoder()
      this.setupPipeline()
      this.setupEncoder()
      this.setupOutput()
   }

   get defaultLabels() {
      return {pipeline: this.pipelineConfig.name}
   }

   help() {
      return {
         input: this.input.help(),
         decoder: this.decoder.help(),
         pipeline: this.pipeline.help(),
         encoder: this.encoder.help(),
         output: this.output.help()
      }
   }

   async start() {
      await this.output.start()
      await this.encoder.start()
      await this.pipeline.start()
      await this.decoder.start()
      await this.input.start()
   }

   async stop() {
      await this.input.stop()
      await this.decoder.stop()
      await this.pipeline.stop()
      await this.encoder.stop()
      await this.output.stop()
   }

   async setupInput() {
      this.input = new Input(this.pipelineConfig)
      this.input
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('in', message => {
            this.globalMessage.inc({...this.defaultLabels, kind:'in'})
         })
         .on('out', message => {
            this.processingMessage.inc({pipeline: this.name})
            this.decoder.in(message)
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

   setupDecoder() {
      this.decoder = new Decoder(this.pipelineConfig)
      this.decoder
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('out', message => {
            this.pipeline.in(message)
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

   setupPipeline() {
      this.pipeline = new Pipeline(this.pipelineConfig)
      this.pipeline
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('out', message => {
            this.encoder.in(message)
         })
         .on('ack', message => {
            this.decoder.ack(message)
         })
         .on('nack', message => {
            this.decoder.nack(message)
         })
         .on('ignore', message => {
            this.decoder.ignore(message)
         })
         .on('reject', message => {
            this.decoder.reject(message)
         })
   }

   setupEncoder() {
      this.encoder = new Encoder(this.pipelineConfig)
      this.encoder
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('out', message => {
            this.output.in(message)
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

   setupOutput() {
      this.output = new Output(this.pipelineConfig)
      this.output
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('ack', message => {
            this.encoder.ack(message)
         })
         .on('nack', message => {
            this.encoder.nack(message)
         })
         .on('ignore', message => {
            this.encoder.ignore(message)
         })
         .on('reject', message => {
            this.encoder.reject(message)
         })
   }
}

module.exports = Processor