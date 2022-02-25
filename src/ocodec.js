const Path     = require('path')
const Loadable = require('./loadable')

class OutputCodec extends Loadable {
   constructor(pipelineConfig) {
      super(pipelineConfig)
      if ( typeof this.executor.encode !== 'function' ) {
         throw new Error(`Codec "${this.config.get('use')}" does not provide "encode" function`)
      }
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './codecs')
      ]
   }

   get options() {
      return this.pipelineConfig.output.codec || {}
   }

   get configSchema() {
      return {
         ...super.configSchema,
         use: {
            doc: '',
            format: String,
            default: 'noop'
         },
         options: this.executorConfigSchema
      }
   }

   // Message -> Data
   async encode(message) {
      if ( typeof this.executor.encode !== 'function' ) {
         return message.content
      }
      return this.executor.encode(message)
   }
}

module.exports = OutputCodec