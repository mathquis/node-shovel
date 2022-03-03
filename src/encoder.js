const Path       = require('path')
const Prometheus = require('prom-client')
const Node       = require('./node')

class Encoder extends Node {
   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './codecs')
      ]
   }

   get options() {
      return this.pipelineConfig.encoder || {}
   }

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

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'encoder_status',
         help: 'Status of the encoder',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'encoder_message',
         help: 'Number of encoder messages',
         labelNames: ['pipeline', 'kind']
      })
   }

   async in(message) {
      this.log.debug('-> ENCODE %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'in'})
      try {
         await this.emit('encode', message)
      } catch (err) {
         this.error(err)
         this.reject(message)
      }
   }
}

module.exports = Encoder