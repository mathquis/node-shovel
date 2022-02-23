const Cluster	= require('cluster')
const Convict	= require('convict')
const Logger	= require('../logger')

class Codec {
	constructor(pipelineConfig, options) {
		options || (options = {})

		this.pipelineConfig = pipelineConfig
		this.config         = Convict(this.configSchema || {})

		const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
		  return $1 + '-' + $2.toLowerCase()
		}).toLowerCase()

		this.log = Logger.child({category: type, worker: Cluster.worker.id, pipeline: this.pipelineConfig.name})

		this.configure(options)
	}

	get configSchema() {
		return {}
	}

	getConfig(key) {
		return this.config.get(key)
	}

	configure(options) {
		this.config.load(options)
		this.config.validate({allowed: 'strict'})

		this.log.debug('%O', this.config.getProperties())
	}

	// Data -> Data
	async decode(content) {
		return content
	}

	// Message -> Data
	async encode(message) {
		return message.content
	}
}

module.exports = Codec