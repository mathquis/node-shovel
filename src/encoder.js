import {fileURLToPath} from 'node:url';
import Path from 'path'
import Prometheus from 'prom-client'
import Node from './node.js'

export default class Encoder extends Node {
   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(Path.dirname(fileURLToPath(import.meta.url)), './encoders')
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
         labelNames: ['pipeline', 'kind', 'type']
      })
   }
}