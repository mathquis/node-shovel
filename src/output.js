const Path        = require('path')
const Prometheus  = require('prom-client')
const Node        = require('./node')
const Codec       = require('./ocodec')

class Output extends Node {
   get configSchema() {
      return {
         ...super.configSchema,
         codec: this.codec.configSchema
      }
   }

   get options() {
      return this.pipelineConfig.output || {}
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './outputs')
      ]
   }

   setup() {
      this.codec = new Codec(this.pipelineConfig)
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'output_status',
         help: 'Status of the output node',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'output_message',
         help: 'Number of output messages',
         labelNames: ['pipeline', 'kind']
      })
   }

   async encode(message) {
      try {
         return await this.codec.encode(message)
      } catch (err) {
         this.error(err)
         this.unack(message)
         return null
      }
   }

   out(message) {
      throw new Error('Output node does not allow outbound message')
   }
}

module.exports = Output