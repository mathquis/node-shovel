const Path       = require('path')
const Prometheus = require('prom-client')
const Node       = require('./node')

class Decoder extends Node {
   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './codecs')
      ]
   }

   get options() {
      return this.pipelineConfig.decoder || {}
   }

   get configSchema() {
      return {
         ...super.configSchema,
         use: {
            doc: '',
            format: String,
            default: 'noop'
         },
         split: {
            doc: '',
            format: Boolean,
            default: false
         }
      }
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'decoder_status',
         help: 'Status of the decoder',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'decoder_message',
         help: 'Number of decoder messages',
         labelNames: ['pipeline', 'kind']
      })
   }

   async in(message) {
      this.log.debug('<- DECODE %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'in'})
      try {
         await this.emit('decode', message)
      } catch (err) {
         this.error(err)
         this.reject(message)
      }
   }

   out(message) {
      if ( !this.config.get('split') || !Array.isArray(message.content) ) {
         super.out(message)
      } else {
         message.content.forEach(content => {
            super.out(message.clone(content))
         })
         this.ack(message)
      }
   }
}

module.exports = Decoder