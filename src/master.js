const Cluster            = require('cluster')
const Koa                = require('koa')
const Router             = require('@koa/router')
const Prometheus         = require('prom-client')
const Config             = require('./config')
const Logger             = require('./logger')
const AggregatorRegistry = require('./aggregated_metrics')

module.exports = async (pipelineConfigs) => {
   const log = Logger.child({category: 'master'})

   function fork(pipelineConfig) {
      const worker = Cluster.fork({
         PIPELINE_PATH: pipelineConfig.file
      })
      log.info('Started worker %d for pipeline: %s', worker.id, pipelineConfig.name)
      workers.set(worker.id, pipelineConfig)
   }

   log.debug('%O', Config.getProperties())
   log.debug('%O', pipelineConfigs)

   const workers = new Map()
   pipelineConfigs.forEach(pipelineConfig => {
      const numWorkers = pipelineConfig.workers
      log.info('Starting %d workers for pipeline: %s', numWorkers, pipelineConfig.name)
      for ( let i = 0 ; i < numWorkers ; i++ ) {
         fork(pipelineConfig)
      }
   })

   let numOnlineWorkers = 0
   const workerGauge = new Prometheus.Gauge({
      name: 'workers',
      help: 'Number of workers',
      labelNames: ['kind']
   })

   workers.set({kind: 'expected'}, workers.size)

   Cluster.on('online', (worker) => {
      const pipelineConfig = workers.get(worker.id)
      log.info('Worker %d is online for pipeline: %s', worker.id, pipelineConfig.name)
      numOnlineWorkers++
      workerGauge.inc({kind: 'online'})
   })

   Cluster.on('exit', (worker, code, signal) => {
      workerGauge.dec({kind: 'online'})
      numOnlineWorkers--
      log.warn(`Worker ${worker.process.pid} died with code: ${code} and signal: ${signal}`)
      if ( code === 1 ) {
         log.info('Starting a new worker...')
         const pipelineConfig = workers.get(worker.id)
         fork(pipelineConfig)
      }
      if ( numOnlineWorkers === 0 ) {
         process.exit(code)
      }
   })

   if ( Config.get('metrics.enabled') ) {
      const registry = new AggregatorRegistry()

      const app = new Koa()
      const router = new Router()

      router.get(Config.get('metrics.route'), async (ctx, next) => {
         try {
            const metrics = await registry.clusterMetrics({
               registries: [Prometheus.register]
            })
            ctx.type = registry.contentType
            ctx.body = metrics
         } catch (err) {
            log.error(err.message)
            ctx.throw(500)
         }
      })

      app
         .use(router.routes())
         .use(router.allowedMethods())

      app.listen( Config.get('metrics.port') )

      log.info(`Prometheus metrics available on ${Config.get('metrics.route')} (port: ${Config.get('metrics.port')})`)
   }
}