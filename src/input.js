const Path        = require('path')
const Prometheus  = require('prom-client')
const Node        = require('./node')
const Message     = require('./message')

class Input extends Node {
   get configSchema() {
      return {
         ...super.configSchema,
         split: {
            doc: '',
            format: Boolean,
            default: true
         }
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

   createMessage(payload, options) {
      options || (options = {})
      const message = new Message(payload, options)
      message.setContentType(options.contentType)
      message.setMetas(options.metas || [])
      console.log(message)
      return message
   }

   in(payload, options = {}) {
       const message = this.createMessage(payload, options)
       super.in(message)
       this.out(message)
   }
}

module.exports = Input