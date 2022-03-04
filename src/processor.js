const Path       = require('path')
const Prometheus = require('prom-client')
const Convict    = require('convict')
const Logger     = require('./logger')
const Utils      = require('./utils')
const Input      = require('./input')
const Decoder    = require('./decoder')
const Queue      = require('./queue')
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
      this.setupQueue()
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
         queue: this.queue.help(),
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
      await this.queue.start()
      await this.input.start()
   }

   async stop() {
      await this.input.stop()
      await this.queue.stop()
      await this.decoder.stop()
      await this.pipeline.stop()
      await this.encoder.stop()
      await this.output.stop()
   }

   async setupInput() {
      this.log.debug('Setting up input')
      try {
         this.input = new Input(this.pipelineConfig)
      } catch (err) {
         this.log.error(err)
      }
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
      this.log.debug('Setting up decoder')
      this.decoder = new Decoder(this.pipelineConfig)
      this.decoder
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('out', message => {
            this.queue.in(message)
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

   setupQueue() {
      this.log.debug('Setting up queue')
      this.queue = new Queue(this.pipelineConfig)
      this.queue
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('queued', message => {
            this.decoder.ack(message)
         })
         .on('out', message => {
            this.pipeline.in(message)
         })
         .on('pause', () => {
            this.input.pause()
         })
         .on('resume', () => {
            this.input.resume()
         })
   }

   setupPipeline() {
      this.log.debug('Setting up pipeline')
      this.pipeline = new Pipeline(this.pipelineConfig)
      this.pipeline
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('out', message => {
            this.encoder.in(message)
         })
         .on('ack', message => {
            this.queue.ack(message)
         })
         .on('nack', message => {
            this.queue.nack(message)
         })
         .on('ignore', message => {
            this.queue.ignore(message)
         })
         .on('reject', message => {
            this.queue.reject(message)
         })
   }

   setupEncoder() {
      this.log.debug('Setting up encoder')
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
      this.log.debug('Setting up output')
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
         .on('pause', () => {
            this.queue.pause()
         })
         .on('resume', () => {
            this.queue.resume()
         })
   }
}

module.exports = Processor