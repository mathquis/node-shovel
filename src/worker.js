const File               = require('fs')
const Path               = require('path')
const Prometheus         = require('prom-client')
const YAML               = require('js-yaml')
const Config             = require('./config')
const Logger             = require('./logger')
const Processor          = require('./processor')
const Help               = require('./help')
const AggregatorRegistry = require('./aggregated_metrics')

module.exports = async (pipelineConfig) => {
  const log = Logger.child({category: 'worker'})

  try {

    Prometheus.collectDefaultMetrics()

    const {name, input, pipeline, output} = pipelineConfig

    const worker = new Processor(name, {
      metrics: {
        labels: Config.get('metrics.labels')
      }
    })

    worker.setupInput(input)
    worker.setupPipeline(pipeline)
    worker.setupOutput(output)

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

    if ( Config.get('help') ) {
      process.stdout.write(Help(worker))
      process.exit()
    } else {
      await worker.start()
    }
  } catch (err) {
    log.error(`${err.message}`)
    process.exit(9)
  }
}