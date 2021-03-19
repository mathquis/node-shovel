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

const pipelineConfig = new PipelineConfig(Config.get('pipeline'))

try {
	pipelineConfig.load()
} catch (err) {
	log.error(err.message)
	process.exit(9)
}

if (Cluster.isMaster) {
  require('./master')(pipelineConfig)
} else {
  require('./worker')(pipelineConfig)
}
