const File	= require('fs')
const Path	= require('path')
const YAML	= require('js-yaml')

class PipelineConfig {
	constructor(pipelineFile) {
		this.file = Path.resolve(process.cwd(), pipelineFile)
	}

	load() {
		try {
			this.config = YAML.load(File.readFileSync(this.file))
		} catch (err) {
			throw new Error(`Invalid pipeline "${this.file}" (${err.message}`)
		}
	}

	loadFn(fn) {
      try {
      	const fnPath = Path.resolve(this.path, fn)
        return require(fnPath)
      } catch (err) {
        throw new Error(`Error loading function "${fn}" (${err.message})`)
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