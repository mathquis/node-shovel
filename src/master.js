const Cluster		= require('cluster')
const Koa			= require('koa')
const Router		= require('@koa/router')
const Prometheus	= require('prom-client')
const Config		= require('./config')
const Logger		= require('./logger')

module.exports = async () => {
	const log = Logger.child({category: 'master'})

	log.debug('%O', Config.getProperties())

	log.info(`Starting ${Config.get('workers')} workers`)

	let numWorkers = 0
	for (let i = 0; i < Config.get('workers'); i++) {
	    Cluster.fork()
	    numWorkers++
	}

	Cluster.on('online', (worker) => {
	    log.info(`Worker ${worker.process.pid} is online`)
	})

	Cluster.on('exit', (worker, code, signal) => {
		numWorkers--
	    log.warn(`Worker ${worker.process.pid} died with code: ${code} and signal: ${signal}`)
	    if ( code === 1 ) {
	    	log.info('Starting a new worker...')
	    	Cluster.fork()
	    	numWorkers++
	    }
	    if ( numWorkers === 0 ) {
	    	process.exit(code)
	    }
	})

	if ( Config.get('metrics.enabled') ) {
		const registry = new Prometheus.AggregatorRegistry()

		const app = new Koa()
		const router = new Router()

		router.get(Config.get('metrics.route'), async (ctx, next) => {
			try {
				const metrics = await registry.clusterMetrics()
				ctx.type = registry.contentType
				ctx.body = metrics
			} catch (err) {
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