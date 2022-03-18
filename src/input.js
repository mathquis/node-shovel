import {fileURLToPath} from 'node:url';
import Path from 'path'
import Prometheus from 'prom-client'
import Node from './node.js'
import Message from './message.js'

export default class Input extends Node {

   get options() {
      return this.pipelineConfig.input
   }

   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(Path.dirname(fileURLToPath(import.meta.url)), './inputs')
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
         labelNames: ['pipeline', 'kind', 'type']
      })
   }

   async in(message) {
      await super.in(message)
      this.out(message)
   }
}
