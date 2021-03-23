const File               = require('fs')
const Path               = require('path')
const Cluster            = require('cluster')
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

    const {name, input, pipeline, output} = pipelineConfig

    log.info('Running pipeline: %s', name)

    const worker = new Processor(pipelineConfig)

    process
      .on('exit', async () => {
        await worker.stop()
      })
      .on('SIGINT', async () => {
        process.exit()
      })
      .on('SIGTERM', async () => {
        process.exit()
      })
      .on('uncaughtException', async err => {
        process.exit(1)
      })

    await worker.start()
  } catch (err) {
    console.error(err)
    log.error(`${err.message}`)
    process.exit(9)
  }
}