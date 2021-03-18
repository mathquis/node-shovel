const File               = require('fs')
const Path               = require('path')
const Prometheus         = require('prom-client')
const YAML               = require('js-yaml')
const Config             = require('./config')
const Logger             = require('./logger')
const Processor          = require('./processor')
const Help               = require('./help')
const AggregatorRegistry = require('./aggregated_metrics')

module.exports = async () => {
  const log = Logger.child({category: 'worker'})

  Prometheus.collectDefaultMetrics()

  // Load pipeline
  if ( !Config.get('pipeline') ) {
    log.error('Missing pipeline')
    process.exit(9)
  }
  let pipelineConfig
  try {
    pipelineConfig = await YAML.load(File.readFileSync(Path.resolve(process.cwd(), Config.get('pipeline'))))
  } catch (err) {
    log.error(`Invalid pipeline "${Config.get('pipeline')}" (${err.message}`)
    process.exit(9)
  }

  try {
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