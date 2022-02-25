const Path        = require('path')
const Prometheus  = require('prom-client')
const Node        = require('./node')
const Message     = require('./message')
const Codec       = require('./icodec')

class Input extends Node {
   get configSchema() {
      return {
         ...super.configSchema,
         split: {
            doc: '',
            format: Boolean,
            default: true
         },
         codec: this.codec.configSchema
      }
   }

   get options() {
      return this.pipelineConfig.input || {}
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './inputs')
      ]
   }

   setup() {
      this.codec = new Codec(this.pipelineConfig)
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'input_status',
         help: 'Status of the input node',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'input_message',
         help: 'Number of input messages',
         labelNames: ['pipeline', 'kind']
      })
   }

   createMessage(data) {
      return new Message(data)
   }

   async decode(data) {
      const contents = await this.codec.decode(data)
      if ( !this.config.get('split') || !Array.isArray(contents) ) {
         return [this.createMessage(contents)]
      }
      return contents.map(content => this.createMessage(content))
   }

   in() {
       this.log.debug('<- IN')
       this.counter.inc({...this.defaultLabels, kind: 'in'})
   }
}

module.exports = Input