import Cluster from 'cluster'
import Config from './core/config.js'
import Logger from './core/logger.js'
import PipelineConfig from './core/pipeline_config.js'
import Master from './core/master.js'
import Worker from './core/worker.js'

Logger.setLogLevel( Config.get('log.level') )

const log = Logger.child({category: 'shovel'})

function loadPipeline(pipelinePath) {
   if ( !pipelinePath ) {
      log.error(`Unknown pipeline "${pipelinePath}"`)
      process.exit(9)
   }
   const pipelineConfig = new PipelineConfig()
   try {
      pipelineConfig.load(pipelinePath)
      log.debug('Loaded pipeline configuration from "%s"', pipelineConfig.file)
      return pipelineConfig
   } catch (err) {
      log.error(err)
      process.exit(9)
   }
}

if (Cluster.isMaster) {
   const pipelinePaths = Config.get('pipeline')
   const pipelineConfigs = pipelinePaths.map(pipelinePath => {
      const pipelineConfig = loadPipeline(pipelinePath)
      log.info('Loaded pipeline configuration from "%s"', pipelineConfig.file)
      return pipelineConfig
   })
   Master(pipelineConfigs)
} else {
   const pipelinePath = process.env.PIPELINE_PATH
   const pipelineConfig = loadPipeline(pipelinePath)
   Worker(pipelineConfig)
}