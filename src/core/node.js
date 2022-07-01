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
      this.isUp      = null
      this.isPaused  = null
      this.lock      = null

      this.emitter = new EventEmitter()

      this.pipelineConfig = pipelineConfig
      this.executorConfigSchema = {
         doc: '',
         format: 'options',
         default: {},
         nullable: true
      }

      this.protocol = protocol

      this.configure(this.options)

      this.log = this.createLogger(this.constructor.name)
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

   createLogger(categoryName) {
      const worker = Cluster.worker && Cluster.worker.id || 0
      const category = categoryName.replace(/(.)([A-Z])/g, (_, $1, $2) => {
         return $1 + '-' + $2.toLowerCase()
      }).toLowerCase()

      return Logger.child({category, worker, pipeline: this.pipelineConfig.name, node: this.name})
   }

   createApi() {
      if ( this.emitter ) {
         this.emitter.removeAllListeners()
      }

      const self = this

      const api = {
         // Logging
         log: {
            debug: (...args) => {
               this.log.debug.apply(this.log, args)
            },
            info: (...args) => {
               this.log.info.apply(this.log, args)
            },
            warn: (...args) => {
               this.log.warn.apply(this.log, args)
            },
            error: (...args) => {
               this.log.error.apply(this.log, args)
            }
         },

         Event: NodeEvent,

         // Properties
         get isStarted() {
            return self.isStarted
         },

         get isUp() {
            return self.isUp
         },

         get isPaused() {
            return self.isPaused
         },

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

         // Functions
         shutdown: () => {
            this.shutdown()
            return api
         },
         broadcast: (pipelines, message) => {
            this.protocol.broadcast(pipelines, message)
            return api
         },
         fanout: (pipelines, message) => {
            this.protocol.fanout(pipelines, message)
            return api
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
            return api
         },
         onStop: handler => {
            this.onEmitter(NodeEvent.STOP, handler)
            return api
         },

         onUp: handler => {
            this.onEmitter(NodeEvent.UP, handler)
            return api
         },
         up: async () => {
            await this.up()
            return api
         },
         onDown: handler => {
            this.onEmitter(NodeEvent.DOWN, handler)
            return api
         },
         down: async () => {
            await this.down()
            return api
         },

         onPause: handler => {
            this.onEmitter(NodeEvent.PAUSE, handler)
            return api
         },
         pause: async () => {
            await this.pause()
            return api
         },
         onResume: handler => {
            this.onEmitter(NodeEvent.RESUME, handler)
            return api
         },
         resume: async () => {
            await this.resume()
            return api
         },

         onIn: handler => {
            this.onEmitter(NodeEvent.IN, handler)
            return api
         },
         in: async (message) => {
            await this.in(message)
            return api
         },
         onOut: handler => {
            this.onEmitter(NodeEvent.OUT, handler)
            return api
         },
         out: async (message) => {
            await this.out(message)
            return api
         },

         onAck: handler => {
            this.onEmitter(NodeEvent.ACK, handler)
            return api
         },
         ack: async (message) => {
            await this.ack(message)
            return api
         },
         onNack: handler => {
            this.onEmitter(NodeEvent.NACK, handler)
            return api
         },
         nack: async (message) => {
            await this.nack(message)
            return api
         },
         onIgnore: handler => {
            this.onEmitter(NodeEvent.IGNORE, handler)
            return api
         },
         ignore: async (message) => {
            await this.ignore(message)
            return api
         },
         onReject: handler => {
            this.onEmitter(NodeEvent.REJECT, handler)
            return api
         },
         reject: async (message) => {
            await this.reject(message)
            return api
         },

         onError: handler => {
            this.onEmitter(NodeEvent.ERROR, handler)
            return api
         },
         error: async (err, message) => {
            await this.error(err, message)
            return api
         }
      }

      return api
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
      const api = this.createApi()
      await loader(api)
      this.isLoaded = true
      return this
   }

   configure(config) {
      config.options || (config.options = {})
      this.config = Convict(this.configSchema || {})
      this.config.load(config)
      try {
         this.config.validate({allowed: 'strict'})
      } catch (err) {
         throw new Error(`${this.constructor.name}[${this.name}] ${err.message}`)
      }
   }

   createMessage(data) {
      return new Message(data)
   }

   shutdown() {
      this.log.info('Shutting down pipeline')
      process.emit('shutdown')
   }

   onEmitter(event, handler) {
      if ( this.emitter.listenerCount(event) > 0 ) {
         this.log.warn('Listener for "%s" already registered', event)
      }
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
            this.pause(true)
         })
      node
         .on(NodeEvent.RESUME, () => {
            this.resume(true)
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
      if ( this.isUp === true ) return
      this.isUp = true
      this.status.set({...this.defaultLabels, kind: 'up'}, 1)
      await this.forwardEvent(NodeEvent.UP)
      this.log.info('"^ Up"')
   }

   async down() {
      if ( this.isUp === false ) return
      this.isUp = false
      this.status.set({...this.defaultLabels, kind: 'up'}, 0)
      await this.forwardEvent(NodeEvent.DOWN)
      this.log.info('"v Down"')
   }

   async pause(lock) {
      if ( this.isPaused === true ) return
      if ( !lock && this.locked ) return
      this.isPaused = true
      this.locked = !!lock
      this.status.set({...this.defaultLabels, kind: 'pause'}, 1)
      await this.forwardEvent(NodeEvent.PAUSE)
      this.log.info('| Paused')
   }

   async resume(lock) {
      if ( this.isPaused === false ) return
      if ( !lock && this.locked ) return
      this.isPaused = false
      this.locked = false
      this.status.set({...this.defaultLabels, kind: 'pause'}, 0)
      await this.forwardEvent(NodeEvent.RESUME)
      this.log.info('> Resumed')
   }

   async error(err, message) {
      if ( this.emitter.listenerCount(NodeEvent.ERROR) > 0 ) {
         await this.emitter.emit(NodeEvent.ERROR, err, message)
      }
      if ( this.listenerCount(NodeEvent.ERROR) > 0 ) {
         await this.emit(NodeEvent.ERROR, err, message)
      }
      let errorMessage = err.stack
      if ( message ) {
         errorMessage += '\n\nMESSAGE:\n' + JSON.stringify(message.toObject(), null, 3)
      }
      this.log.error(errorMessage)
      this.counter.inc({...this.defaultLabels, kind: NodeEvent.ERROR})
   }

   async forwardEvent(event, message) {
      let shouldPropagate = true
      try {
         if ( this.emitter.listenerCount(event) > 0 ) {
            const results = await this.emitter.emit(event, message)
            shouldPropagate = results.indexOf(false) < 0
         }
         this.log.debug('Checking propagation (event: %s, forward: %s)', event, shouldPropagate)
         if ( shouldPropagate ) {
            this.counter.inc({...this.defaultLabels, kind: event})
            if ( this.listenerCount(event) > 0 ) {
               await this.emit(event, message)
            }
         }
      } catch (err) {
         this.error(err, message)
         if ( message && event !== NodeEvent.REJECT ) {
            this.reject(message)
         }
      }
      return shouldPropagate
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