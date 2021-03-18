const File			= require('fs')
const Path			= require('path')
const Prometheus	= require('prom-client')
const YAML			= require('js-yaml')
const Config		= require('./config')
const Logger 		= require('./logger')
const Pipeline		= require('./pipeline')

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

	// const indexTemplate = IndexTemplate(Config.get('elasticsearch.template_name'))
	let worker
	try {
		worker = new Pipeline(pipelineConfig)
	} catch (err) {
		log.error(`${err.message}`)
		process.exit(9)
	}

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

	await worker.start()
}