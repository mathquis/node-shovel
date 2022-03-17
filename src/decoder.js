import {fileURLToPath} from 'node:url';
import Path from 'path'
import Prometheus from 'prom-client'
import Node from './node.js'

export default class Decoder extends Node {
   get includePaths() {
      return [
         ...super.includePaths,
         Path.resolve(Path.dirname(fileURLToPath(import.meta.url)), './decoders')
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
         labelNames: ['pipeline', 'kind', 'type']
      })
   }
}