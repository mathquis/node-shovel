const File               = require('fs')
const Path               = require('path')
const Cluster            = require('cluster')
const Readline           = require('readline')
const Prometheus         = require('prom-client')
const YAML               = require('js-yaml')
const Config             = require('./config')
const Logger             = require('./logger')
const Processor          = require('./processor')
const Help               = require('./help')
const AggregatorRegistry = require('./aggregated_metrics')

module.exports = async (pipelineConfig) => {
   const log = Logger.child({category: 'worker', pipeline: pipelineConfig.name, worker: Cluster.worker.id})

   try {

      Prometheus.collectDefaultMetrics()

      log.info('Running pipeline "%s"', pipelineConfig.name)

      const worker = new Processor(pipelineConfig)

      if ( process.platform === 'win32' ) {
        var rl = Readline.createInterface({
          input: process.stdin,
          output: process.stdout
        });

        rl.on('SIGINT', function () {
          process.emit('SIGINT');
        });
      }

      process
         .on('asyncExit', async () => {
            await worker.stop()
            process.exit()
         })
         .on('exit', () => {
            worker.stop()
         })
         .on('SIGINT', async () => {
            process.emit('asyncExit')
         })
         .on('SIGTERM', async () => {
            process.emit('asyncExit')
         })
         .on('uncaughtException', async err => {
            log.error(err)
            process.exit(1)
         })

      await worker.start()
   } catch (err) {
      log.error(err.stack)
      process.exit(9)
   }
}