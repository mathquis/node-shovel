import EventEmitter from 'events-async'
import Cluster from 'cluster'
import Convict from 'convict'
import Logger from './logger.js'
import Utils from './utils.js'

export default class Loadable extends EventEmitter {
   constructor(pipelineConfig) {
      super()

      this.isLoaded  = false

      this.pipelineConfig = pipelineConfig
      this.executorConfigSchema = {
         doc: '',
         format: 'options',
         default: {},
         nullable: true
      }

      this.configure(this.options)

      this.setupLogger()

      this.log.debug('%O', this.config.get('options'))
   }

   get util() {
      return Utils
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

      this.loader(this)

      this.isLoaded = true

      return this
   }

   getConfig(key) {
      if ( !key ) {
         return this.config.get('options')
      }
      return this.config.get('options.' + key)
   }

   registerConfig(schema) {
      this.executorConfigSchema = schema
      this.configure(this.options)
      return this
   }

   configure(config) {
      config.options || (config.options = {})
      this.config = Convict(this.configSchema || {})
      this.config.load(config)
      this.config.validate({allowed: 'strict'})
   }

   setupLogger() {
      const worker = Cluster.worker && Cluster.worker.id || 0
      const category = `${this.constructor.name}-${this.config.get('use')}`.replace(/(.)([A-Z])/g, (_, $1, $2) => {
         return $1 + '-' + $2.toLowerCase()
      }).toLowerCase()

      this.log = Logger.child({category, worker, pipeline: this.pipelineConfig.name})
   }
}
