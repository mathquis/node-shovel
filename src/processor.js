import Path from 'path'
import Prometheus from 'prom-client'
import Convict from 'convict'
import Logger from './logger.js'
import Utils from './utils.js'
import Input from './input.js'
import Decoder from './decoder.js'
import Queue from './queue.js'
import Pipeline from './pipeline.js'
import Encoder from './encoder.js'
import Output from './output.js'

const registers = []

export default class Processor {
   constructor(pipelineConfig, protocol) {
      this.pipelineConfig = pipelineConfig
      this.protocol = protocol

      this.log = Logger.child({category: this.pipelineConfig.name})

      const labelNames = ['pipeline', 'kind', 'type']

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
      this.setupQueue()
      this.setupOutput()

      this.input
         .pipe(this.decoder)
         .pipe(this.pipeline)
         .pipe(this.encoder)
         .pipe(this.queue)
         .pipe(this.output)
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
         queue: this.queue.help(),
         output: this.output.help()
      }
   }

   async getMessageProcessedMetric() {
      return this.globalMessage.get()
   }

   async start() {
      await this.output.start()
      await this.queue.start()
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
      await this.queue.stop()
      await this.output.stop()
   }

   in(data) {
      const message = this.input.createMessage()
      message.fromObject(data)
      this.globalMessage.inc({...this.defaultLabels, kind:'in'})
      this.processingMessage.inc({pipeline: this.name})
      this.pipeline.in(message)
   }

   async setupInput() {
      this.log.debug('Setting up input')
      try {
         this.input = new Input(this.pipelineConfig, this.protocol)
      } catch (err) {
         this.log.error(err)
      }
      this.input
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
         .on('in', message => {
            this.globalMessage.inc({...this.defaultLabels, kind:'in'})
            this.processingMessage.inc({pipeline: this.name})
         })
         .on('ack', message => {
            this.processingMessage.dec({pipeline: this.name})
            this.globalMessage.inc({...this.defaultLabels, kind: 'acked'})
         })
         .on('nack', message => {
            this.processingMessage.dec({pipeline: this.name})
            this.globalMessage.inc({...this.defaultLabels, kind: 'nacked'})
         })
         .on('ignore', message => {
            this.processingMessage.dec({pipeline: this.name})
            this.globalMessage.inc({...this.defaultLabels, kind: 'ignored'})
         })
         .on('reject', message => {
            this.processingMessage.dec({pipeline: this.name})
            this.globalMessage.inc({...this.defaultLabels, kind: 'rejected'})
         })
   }

   async setupDecoder() {
      this.log.debug('Setting up decoder')
      this.decoder = new Decoder(this.pipelineConfig, this.protocol)
      this.decoder
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
   }

   async setupPipeline() {
      this.log.debug('Setting up pipeline')
      this.pipeline = new Pipeline(this.pipelineConfig, this.protocol)
      this.pipeline
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
   }

   async setupEncoder() {
      this.log.debug('Setting up encoder')
      this.encoder = new Encoder(this.pipelineConfig, this.protocol)
      this.encoder
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
   }

   async setupQueue() {
      this.log.debug('Setting up queue')
      this.queue = new Queue(this.pipelineConfig, this.protocol)
      this.queue
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
   }

   async setupOutput() {
      this.log.debug('Setting up output')
      this.output = new Output(this.pipelineConfig, this.protocol)
      this.output
         .on('error', err => {
            this.globalMessage.inc({...this.defaultLabels, kind: 'error'})
         })
   }
}