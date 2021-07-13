const File		= require('fs')
const Path		= require('path')
const YAML		= require('js-yaml')
const Logger	= require('../logger')
const Utils 	= require('../utils')

class PipelineConfig {
	constructor(pipelineFile) {
		this.file = Path.resolve(process.cwd(), pipelineFile)

    const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
      return $1 + '-' + $2.toLowerCase()
    }).toLowerCase()

    this.log = Logger.child({category: type})
	}

	load() {
		try {
			this.log.info('Loading pipeline configuration at "%s"', this.file)
			this.config = YAML.load(File.readFileSync(this.file))
		} catch (err) {
			throw new Error(`Invalid pipeline "${this.file}" (${err.message}`)
		}
	}

	get path() {
		return Path.dirname(Path.resolve(this.file))
	}

	get name() {
		return this.config.name || '<none>'
	}

	get workers() {
		return this.config.workers || 1
	}

	get input() {
		return this.config.input || {}
	}

	get pipeline() {
		return this.config.pipeline || {}
	}

	get output() {
		return this.config.output || {}
	}

	toJSON() {
		return this.config
	}
}

module.exports = PipelineConfig