const Path        = require('path')
const Prometheus  = require('prom-client')
const Node        = require('./node')
const Message     = require('./message')

class Queue extends Node {
   get configSchema() {
      return {
         ...super.configSchema,
         use: {
            doc: '',
            format: String,
            default: 'memory'
         }
      }
   }

   get options() {
      return this.pipelineConfig.queue || {}
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(__dirname, './queues')
      ]
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'queue_status',
         help: 'Status of the queue node',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'queue_message',
         help: 'Number of queue messages',
         labelNames: ['pipeline', 'kind']
      })
   }

   async queued(message) {
      this.log.debug('== QUEUED %s', message)
      this.emit('queued', message)
   }

   // async evicted(message) {
   //    this.log.debug('!! EVICTED %s', message)
   //    this.emit('evicted', message)
   //    this.counter.inc({...this.defaultLabels, kind: 'evicted'})
   // }

   async pause() {
      if ( !this.isUp ) return
      if ( this.isPaused ) return
      this.isPaused = true
      this.log.debug('| Paused')
      await this.emit('pause')
      this.counter.inc({...this.defaultLabels, kind: 'pause'})
   }

   async resume() {
      if ( !this.isUp ) return
      if ( !this.isPaused ) return
      this.isPaused = false
      this.log.debug('> Resumed')
      await this.emit('resume')
      this.counter.inc({...this.defaultLabels, kind: 'resume'})
   }

   // async drain() {
   //    this.log.debug(':: DRAINED')
   //    this.emit('drain')
   // }
}

module.exports = Queue