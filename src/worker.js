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
      .on('SIGINT', async () => {
        await worker.stop()
        process.exit()
      })
      .on('SIGTERM', async () => {
        await worker.stop()
        process.exit()
      })
      .on('uncaughtException', async err => {
        await worker.stop()
        process.exit(1)
      })
  } catch (err) {
    console.error(err)
    log.error(`${err.message}`)
    process.exit(9)
  }
}