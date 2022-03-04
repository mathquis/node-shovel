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
      return message
   }

   async in(payload, options = {}) {
       const message = this.createMessage(payload, options)
       await super.in(message)
       await this.out(message)
   }

   async pause() {
      if ( !this.isUp ) return
      if ( this.isPaused ) return
      this.isPaused = true
      this.log.info('| Paused')
      await this.emit('pause')
      this.counter.inc({...this.defaultLabels, kind: 'pause'})
   }

   async resume() {
      if ( !this.isUp ) return
      if ( !this.isPaused ) return
      this.isPaused = false
      this.log.info('> Resumed')
      await this.emit('resume')
      this.counter.inc({...this.defaultLabels, kind: 'resume'})
   }
}

module.exports = Input