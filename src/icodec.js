const Path     = require('path')
const Loadable = require('./loadable')

class InputCodec extends Loadable {
   constructor(pipelineConfig) {
      super(pipelineConfig)
      if ( typeof this.executor.decode !== 'function' ) {
         throw new Error(`Codec "${this.config.get('use')}" does not provide "decode" function`)
      }
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './codecs')
      ]
   }

   get options() {
      return this.pipelineConfig.input.codec || {}
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

   // Data -> Data
   async decode(content) {
      return this.executor.decode(content)
   }
}

module.exports = InputCodec