const Path        = require('path')
const Prometheus  = require('prom-client')
const Node        = require('./node')

class Output extends Node {
   get options() {
      return this.pipelineConfig.output || {}
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './outputs')
      ]
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

   out(message) {
      throw new Error('Output node does not allow outbound message')
   }
}

module.exports = Output