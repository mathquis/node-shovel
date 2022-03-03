const EventEmitter = require('events-async')
const Cluster      = require('cluster')
const Convict      = require('convict')
const Logger       = require('./logger')
const Utils        = require('./utils')

class Loadable extends EventEmitter {
   constructor(pipelineConfig) {
      super()

      this.pipelineConfig   = pipelineConfig
      this.executorConfigSchema = {
         doc: '',
         format: 'options',
         default: {},
         nullable: true
      }

      this.setup()

      this.configure(this.options)

      this.setupLogger()

      this.kind = this.config.get('use')
      if (!this.kind) {
         throw new Error(`Missing node kind`)
      }

      this.loader = Utils.loadFn(this.kind, this.includePaths)

      if ( typeof this.loader !== 'function' ) {
         throw new Error(`Invalid node "${this.kind}" (not a function)`)
      }

      this.executor = this.loader(this)

      this.log.debug('%O', this.config.get('options'))
   }

   get util() {
      return Utils
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

   setup() {

   }

   setupLogger() {
      const worker = Cluster.worker.id
      const category = `${this.constructor.name}-${this.config.get('use')}`.replace(/(.)([A-Z])/g, (_, $1, $2) => {
         return $1 + '-' + $2.toLowerCase()
      }).toLowerCase()

      this.log = Logger.child({category, worker, pipeline: this.pipelineConfig.name})
   }

   help() {
      return this.config.getSchema()
   }
}

module.exports = Loadable