const Cluster            = require('cluster')
const Koa                = require('koa')
const Router             = require('@koa/router')
const Prometheus         = require('prom-client')
const Config             = require('./config')
const Logger             = require('./logger')
const AggregatorRegistry = require('./aggregated_metrics')

module.exports = async (pipelineConfig) => {
  const log = Logger.child({category: 'master'})

  if ( Config.get('help') ) {
    Cluster.fork()
    Cluster.on('exit', () => {
      process.exit()
    })
    return
  }

  log.debug('%O', Config.getProperties())
  log.info('%O', pipelineConfig)

  const {name: pipeline} = pipelineConfig

  let numOnlineWorkers = 0
  const workers = new Prometheus.Gauge({
    name: 'workers',
    help: 'Number of online workers',
    labelNames: ['pipeline', 'kind']
  })

  workers.set({pipeline, kind: 'expected'}, Config.get('workers'))

  log.info(`Starting ${Config.get('workers')} workers`)

  let numWorkers = 0
  for (let i = 0; i < Config.get('workers'); i++) {
      Cluster.fork()
  }

  Cluster.on('online', (worker) => {
      log.info(`Worker ${worker.process.pid} is online`)
      numOnlineWorkers++
      workers.inc({pipeline, kind: 'online'})
  })

  Cluster.on('exit', (worker, code, signal) => {
    workers.dec({pipeline, kind: 'online'})
    numOnlineWorkers--
      log.warn(`Worker ${worker.process.pid} died with code: ${code} and signal: ${signal}`)
      if ( code === 1 ) {
        log.info('Starting a new worker...')
        Cluster.fork()
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