import Cluster from 'cluster'
import Convict from 'convict'
import EventEmitter from 'promise-events'
import Prometheus from 'prom-client'
import Logger from './logger.js'
import Utils from './utils.js'
// import Loadable from './loadable.js'
import { NodeEvent } from './event.js'
import Message from './message.js'

export default class NodeOperator extends EventEmitter {
   constructor(pipelineConfig, protocol) {
      super()

      this.isLoaded  = false
      this.isStarted = false
      this.isUp      = false
      this.isPaused  = false

      this.emitter = new EventEmitter()

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

   static get Event() {
      return NodeEvent
   }

   get api() {
      const log = this.createLogger(`${this.constructor.name}[${this.name}]`)

      const api = {
         // Logging
         log,

         Event: NodeEvent,

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
            this.emitter.on(event, handler)
            return api
         },
         once: (event, handler) => {
            this.emitter.once(event, handler)
            return api
         },
         off: (event, handler) => {
            this.emitter.off(event, handler)
            return api
         },

         // Interface
         onStart: handler => {
            this.onEmitter(NodeEvent.START, handler)
         },
         start: () => this.start(),
         onStop: handler => {
            this.onEmitter(NodeEvent.STOP, handler)
         },
         stop: () => this.stop(),
         shutdown: () => this.shutdown(),
         onUp: handler => {
            this.onEmitter(NodeEvent.UP, handler)
         },
         up: () => this.up(),
         onDown: handler => {
            this.onEmitter(NodeEvent.DOWN, handler)
         },
         down: () => this.down(),
         onPause: handler => {
            this.onEmitter(NodeEvent.PAUSE, handler)
         },
         pause: () => this.pause(),
         onResume: handler => {
            this.onEmitter(NodeEvent.RESUME, handler)
         },
         resume: () => this.resume(),
         onIn: handler => {
            this.onEmitter(NodeEvent.IN, handler)
         },
         in: (message) => this.in(message),
         onOut: handler => {
            this.onEmitter(NodeEvent.OUT, handler)
         },
         out: (message) => this.out(message),
         onAck: handler => {
            this.onEmitter(NodeEvent.ACK, handler)
         },
         ack: (message) => this.ack(message),
         onNack: handler => {
            this.onEmitter(NodeEvent.NACK, handler)
         },
         nack: (message) => this.nack(message),
         onIgnore: handler => {
            this.onEmitter(NodeEvent.IGNORE, handler)
         },
         ignore: (message) => this.ignore(message),
         onReject: handler => {
            this.onEmitter(NodeEvent.REJECT, handler)
         },
         reject: (message) => this.reject(message),
         onError: handler => {
            this.onEmitter(NodeEvent.ERROR, handler)
         },
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

      const loader = await Utils.loadFn(this.name, this.includePaths)

      if ( typeof loader !== 'function' ) {
         throw new Error(`Invalid node "${this.name}" (not a function)`)
      }

      return this.set(loader)
   }

   async set(loader) {
      await loader(this.api)
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

   onEmitter(event, handler) {
      this.emitter.on(event, handler)
   }

   pipe(node) {
      this
         .on(NodeEvent.OUT, async message => {
            node.in(message)
         })

      node
         .on(NodeEvent.ACK, message => {
            this.ack(message)
         })

      node
         .on(NodeEvent.NACK, message => {
            this.nack(message)
         })
      node
         .on(NodeEvent.IGNORE, message => {
            this.ignore(message)
         })
      node
         .on(NodeEvent.REJECT, message => {
            this.reject(message)
         })
      node
         .on(NodeEvent.PAUSE, () => {
            this.pause()
         })
      node
         .on(NodeEvent.RESUME, () => {
            this.resume()
         })

      return node
   }

   async start() {
      if ( this.isStarted ) return
      this.isStarted = true
      this.log.debug('Started')
      const forward = await this.forwardEvent(NodeEvent.START)
      if ( forward ) {
         await this.up()
      }
   }

   async stop() {
      if ( !this.isStarted ) return
      await this.down()
      await this.forwardEvent(NodeEvent.STOP)
      this.isStarted = false
      this.log.debug('Stopped')
   }

   async up() {
      if ( this.isUp ) return
      this.isUp = true
      this.status.set({...this.defaultLabels, kind: 'up'}, 1)
      await this.forwardEvent(NodeEvent.UP)
      this.log.info('"^ Up"')
   }

   async down() {
      if ( !this.isUp ) return
      this.isUp = false
      this.status.set({...this.defaultLabels, kind: 'down'}, 0)
      await this.forwardEvent(NodeEvent.DOWN)
      this.log.info('"v Down"')
   }

   async pause() {
      if ( !this.isUp ) return
      if ( this.isPaused ) return
      this.isPaused = true
      await this.forwardEvent(NodeEvent.PAUSE)
      this.log.info('| Paused')
   }

   async resume() {
      if ( !this.isUp ) return
      if ( !this.isPaused ) return
      this.isPaused = false
      await this.forwardEvent(NodeEvent.RESUME)
      this.log.info('> Resumed')
   }

   async error(err, message) {
      let errorMessage = err.stack
      if ( message ) {
         errorMessage += '\n\nMESSAGE:\n' + JSON.stringify(message.toObject(), null, 3)
      }
      this.log.error(errorMessage)
      this.counter.inc({...this.defaultLabels, kind: NodeEvent.ERROR})
      await this.emit(NodeEvent.ERROR, err, message)
   }

   async forwardEvent(event, message) {
      try {
         this.counter.inc({...this.defaultLabels, kind: event})
         let shouldPropagate = true
         if ( this.emitter.listenerCount(event) > 0 ) {
            const results = await this.emitter.emit(event, message)
            shouldPropagate = results.indexOf(false) < 0
         }
         this.log.debug('Checking propagation (event: %s, forward: %s)', event, shouldPropagate)
         if ( shouldPropagate ) {
            await this.emit(event, message)
         }
         return shouldPropagate
      } catch (err) {
         this.error(err, message)
         if ( event !== NodeEvent.REJECT ) {
            this.reject(message)
         }
         return true
      }
   }

   in(message) {
      this.log.debug('<- IN %s', message)
      return this.forwardEvent(NodeEvent.IN, message)
   }

   out(message) {
      this.log.debug('-> OUT %s', message)
      return this.forwardEvent(NodeEvent.OUT, message)
   }

   ack(message) {
      this.log.debug('-+ ACK %s', message)
      return this.forwardEvent(NodeEvent.ACK, message)
   }

   nack(message) {
      this.log.debug('-X NACK %s', message)
      return this.forwardEvent(NodeEvent.NACK, message)
   }

   ignore(message) {
      this.log.debug('-- IGNORE %s', message)
      return this.forwardEvent(NodeEvent.IGNORE, message)
   }

   reject(message) {
      this.log.debug('-! REJECT %s', message)
      return this.forwardEvent(NodeEvent.REJECT, message)
   }
}