import {fileURLToPath} from 'node:url';
import Path from 'path'
import Prometheus from 'prom-client'
import Node from './node.js'

export default class Queue extends Node {
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
      return this.pipelineConfig.queue
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(Path.dirname(fileURLToPath(import.meta.url)), './queues')
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
         labelNames: ['pipeline', 'kind', 'type']
      })
   }

   evict(message) {
      this.log.debug('// EVICTED %s', message)
      this.emit('evict', message)
      this.counter.inc({...this.defaultLabels, kind: 'evicted'})
   }
}
