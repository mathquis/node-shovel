const Path		= require('path')
const Cluster	= require('cluster')
const Convict	= require('convict')
const Logger	= require('./logger')
const Utils		= require('./utils')
const NoopCodec	= require('./codecs/noop')

class CodecOperator {
	constructor(use, pipelineConfig, options) {
		options || (options = {})

		this.pipelineConfig = pipelineConfig

	    let codecInc = NoopCodec
	    if ( use ) {
	      try {
	        codecInc = Utils.loadFn(use, [Path.resolve(__dirname, './codecs'), pipelineConfig.path])
	      } catch (err) {
	        throw new Error(`Unknown codec "${use} (${err.message})`)
	      }
	    }

        if (!codecInc.codec) {
        	throw new Error(`Invalid codec "${use} (missing codec function)`)
        }

		const type = (`codec-${use}`).replace(/(.)([A-Z])/g, (_, $1, $2) => {
		  return $1 + '-' + $2.toLowerCase()
		}).toLowerCase()

		this.log = Logger.child({category: type, worker: Cluster.worker.id, pipeline: this.pipelineConfig.name})

		this.log.info('Using codec: %s', use)

		this.config = Convict(codecInc.configSchema || {})
		this.configure(options)

    	this.codec = codecInc.codec(this, options)
	}

	get configSchema() {
		return {
			...this.codec.configSchema
		}
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
		return this.codec.decode(content)
	}

	// Message -> Data
	async encode(message) {
		return this.codec.encode(message)
	}
}

module.exports = CodecOperator