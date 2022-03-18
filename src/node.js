import Cluster from 'cluster'
import Convict from 'convict'
import EventEmitter from 'events-async'
import Prometheus from 'prom-client'
import Logger from './logger.js'
import Utils from './utils.js'
// import Loadable from './loadable.js'
import Message from './message.js'

export default class NodeOperator extends EventEmitter {
   constructor(pipelineConfig, protocol) {
      super()

      this.isLoaded  = false
      this.isStarted = false
      this.isUp      = false
      this.isPaused  = false


      this.pipelineConfig = pipelineConfig
      this.executorConfigSchema = {
         doc: '',
         format: 'options',
         default: {},
         nullable: true
      }

      this.protocol = protocol

      this.log = this.createLogger(this.constructor.name)

      this.configure(this.options)

      this.log.debug('%O', this.config.get('options'))

      this.setupMonitoring()
   }

   get name() {
      return this.config.get('use')
   }

   get options() {
      return {}
   }

   get includePaths() {
      return [this.pipelineConfig.path]
   }

   get configSchema() {
      return {
         use: {
            doc: '',
            format: String,
            default: ''
         },
         options: {
            ...this.executorConfigSchema
         }
      }
   }

   get defaultLabels() {
      return {pipeline: this.pipelineConfig.name}
   }

   get api() {
      const log = this.createLogger(`${this.constructor.name}[${this.name}]`)

      const api = {
         // Logging
         log,

         // Configuration
         pipelineConfig: this.pipelineConfig,
         registerConfig: (schema) => {
            this.executorConfigSchema = schema
            this.configure(this.options)
            return api
         },
         getConfig: key => {
            if ( !key ) {
               return this.config.get('options')
            }
            return this.config.get('options.' + key)
         },

         // Helpers
         util: Utils,
         createMessage: (data) => {
            return new Message(data)
         },

         // Events
         on: (event, handler) => {
            this.on(event, handler)
            return api
         },
         once: (event, handler) => {
            this.once(event, handler)
            return api
         },
         off: (event, handler) => {
            this.off(event, handler)
            return api
         },

         // Interface
         start: () => this.start(),
         stop: () => this.stop(),
         shutdown: () => this.shutdown(),
         up: () => this.up(),
         down: () => this.down(),
         pause: () => this.pause(),
         resume: () => this.resume(),
         in: (message) => this.in(message),
         out: (message) => this.out(message),
         ack: (message) => this.ack(message),
         nack: (message) => this.nack(message),
         ignore: (message) => this.ignore(message),
         reject: (message) => this.reject(message),
         error: (err, message) => this.error(err, message),
         broadcast: (pipelines, message) => {
            this.protocol.broadcast(pipelines, message)
         },
         fanout: (pipelines, message) => {
            this.protocol.fanout(pipelines, message)
         }
      }

      return api
   }

   createLogger(categoryName) {
      const worker = Cluster.worker && Cluster.worker.id || 0
      const category = categoryName.replace(/(.)([A-Z])/g, (_, $1, $2) => {
         return $1 + '-' + $2.toLowerCase()
      }).toLowerCase()

      return Logger.child({category, worker, pipeline: this.pipelineConfig.name})
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'node_status',
         help: 'Status of the node',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'node_message',
         help: 'Number of messages',
         labelNames: ['pipeline', 'kind', 'type']
      })
   }

   async load() {
      if ( this.isLoaded ) {
         return
      }

      if (!this.name) {
         throw new Error(`Missing node kind`)
      }

      this.loader = await Utils.loadFn(this.name, this.includePaths)

      if ( typeof this.loader !== 'function' ) {
         throw new Error(`Invalid node "${this.name}" (not a function)`)
      }

      this.loader(this.api)

      this.isLoaded = true

      return this
   }

   configure(config) {
      config.options || (config.options = {})
      this.config = Convict(this.configSchema || {})
      this.config.load(config)
      this.config.validate({allowed: 'strict'})
   }

   createMessage(data) {
      return new Message(data)
   }

   shutdown() {
      this.log.info('Shutting down pipeline')
      process.emit('shutdown')
   }

   pipe(node) {
      this
         .on('out', async message => {
            node.in(message)
         })

      node
         .on('ack', message => {
            this.ack(message)
         })
         .on('nack', message => {
            this.nack(message)
         })
         .on('ignore', message => {
            this.ignore(message)
         })
         .on('reject', message => {
            this.reject(message)
         })
         .on('pause', () => {
            this.pause()
         })
         .on('resume', () => {
            this.resume()
         })

      return node
   }

   async start() {
      if ( this.isStarted ) return
      this.isStarted = true
      this.log.debug('Started')
      const results = await this.emit('start')
      if ( !results ) {
         await this.up()
      }
   }

   async stop() {
      if ( !this.isStarted ) return
      await this.down()
      await this.emit('stop')
      this.isStarted = false
      this.log.debug('Stopped')
   }

   async up() {
      if ( this.isUp ) return
      this.isUp = true
      this.status.set({...this.defaultLabels, kind: 'up'}, 1)
      await this.emit('up')
      this.log.info('"^ Up"')
   }

   async down() {
      if ( !this.isUp ) return
      this.isUp = false
      this.status.set({...this.defaultLabels, kind: 'down'}, 0)
      await this.emit('down')
      this.log.info('"v Down"')
   }

   async pause() {
      if ( !this.isUp ) return
      if ( this.isPaused ) return
      this.isPaused = true
      this.log.info('| Paused')
      await this.emit('pause')
      this.counter.inc({...this.defaultLabels, kind: 'pause'})
   }

   async resume() {
      if ( !this.isUp ) return
      if ( !this.isPaused ) return
      this.isPaused = false
      this.log.info('> Resumed')
      await this.emit('resume')
      this.counter.inc({...this.defaultLabels, kind: 'resume'})
   }

   error(err) {
      let errorMessage = err.stack
      if ( err.origin ) {
         errorMessage += '\n\nMESSAGE:\n' + JSON.stringify(err.origin, null, 3)
      }
      this.log.error(errorMessage)
      this.counter.inc({...this.defaultLabels, kind: 'error'})
      this.emit('error', err)
   }

   async in(message) {
      this.log.debug('<- IN %s', message)
      this.counter.inc({...this.defaultLabels, kind: 'in'})
      try {
         await this.emit('in', message)
      } catch (err) {
         err.origin = message
         this.error(err)
         this.reject(message)
      }
   }

   out(message) {
      this.log.debug('-> OUT %s', message)
      this.emit('out', message)
      this.counter.inc({...this.defaultLabels, kind: 'out'})
   }

   ack(message) {
      this.log.debug('-+ ACK %s', message)
      this.emit('ack', message)
      this.counter.inc({...this.defaultLabels, kind: 'acked'})
   }

   nack(message) {
      this.log.debug('-X NACK %s', message)
      this.emit('nack', message)
      this.counter.inc({...this.defaultLabels, kind: 'nacked'})
   }

   ignore(message) {
      this.log.debug('-- IGNORE %s', message)
      this.emit('ignore', message)
      this.counter.inc({...this.defaultLabels, kind: 'ignored'})
   }

   reject(message) {
      this.log.debug('-! REJECT %s', message)
      this.emit('reject', message)
      this.counter.inc({...this.defaultLabels, kind: 'rejected'})
   }
}