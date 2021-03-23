const Cluster			= require('cluster')
const Config			= require('./config')
const Logger			= require('./logger')
const PipelineConfig	= require('./pipelines/config')

const log = Logger.child({category: 'shovel'})

function loadPipeline(pipelinePath) {
  if ( !pipelinePath ) {
    log.error(`Unknown pipeline "${pipelinePath}"`)
    process.exit(9)
  }
  const pipelineConfig = new PipelineConfig(pipelinePath)
  try {
    pipelineConfig.load()
    return pipelineConfig
  } catch (err) {
    log.error(err.message)
    process.exit(9)
  }
}

if (Cluster.isMaster) {
  const pipelinePaths = Config.get('pipeline')
  const pipelineConfigs = pipelinePaths.map(loadPipeline)
  require('./master')(pipelineConfigs)
} else {
  const pipelinePath = process.env.PIPELINE_PATH
  const pipelineConfig = loadPipeline(pipelinePath)
  require('./worker')(pipelineConfig)
}
