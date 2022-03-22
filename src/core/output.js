import {fileURLToPath} from 'node:url';
import Path from 'path'
import Prometheus from 'prom-client'
import Node from './node.js'

export default class Output extends Node {
   get options() {
      return this.pipelineConfig.output
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(Path.dirname(fileURLToPath(import.meta.url)), '../outputs')
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
         labelNames: ['pipeline', 'kind', 'type']
      })
   }

   ack(message) {
      this.out(message)
      super.ack(message)
   }

   ignore(message) {
      this.out(message)
      super.ignore(message)
   }

   reject(message) {
      this.out(message)
      super.reject(message)
   }
}