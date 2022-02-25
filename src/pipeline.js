const Path        = require('path')
const Prometheus  = require('prom-client')
const Node        = require('./node')

class Pipeline extends Node {

   get configSchema() {
      return {
         ...super.configSchema,
         use: {
            doc: '',
            format: String,
            default: 'noop'
         }
      }
   }

   get options() {
      return this.pipelineConfig.pipeline || {}
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './pipelines')
      ]
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'pipeline_status',
         help: 'Status of the pipeline node',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'pipeline_message',
         help: 'Number of pipeline messages',
         labelNames: ['pipeline', 'kind']
      })
   }
}

module.exports = Pipeline