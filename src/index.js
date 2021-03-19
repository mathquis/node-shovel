const Cluster			= require('cluster')
const Config			= require('./config')
const Logger			= require('./logger')
const PipelineConfig	= require('./pipelines/config')

const log = Logger.child({category: 'shovel'})

// Load pipeline
if ( !Config.get('pipeline') ) {
  log.error('Missing pipeline')
  process.exit(9)
}

if (Cluster.isMaster) {
  const pipelineConfigs = Config.get('pipeline').map(pipeline => {
    const pipelineConfig = new PipelineConfig(pipeline)
    try {
      pipelineConfig.load()
    } catch (err) {
      log.error(err.message)
      process.exit(9)
    }
    return pipelineConfig
  })
  require('./master')(pipelineConfigs)
} else {
  const pipelinePath = process.env.PIPELINE_PATH
  const pipelineConfig = new PipelineConfig(pipelinePath)
  if ( !pipelineConfig ) {
  	log.error(`Unknown pipeline "${pipelinePath}"`)
  	process.exit(9)
  }
  try {
    pipelineConfig.load()
  } catch (err) {
    log.error(err.message)
    process.exit(9)
  }
  require('./worker')(pipelineConfig)
}
