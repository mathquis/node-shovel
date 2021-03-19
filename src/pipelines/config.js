const File	= require('fs')
const Path	= require('path')
const YAML	= require('js-yaml')

class PipelineConfig {
	constructor(path) {
		this._path = Path.resolve(process.cwd(), path)
	}

	load() {
		try {
			this.config = YAML.load(File.readFileSync(this._path))
		} catch (err) {
			throw new Error(`Invalid pipeline "${this._path}" (${err.message}`)
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
		return Path.dirname(Path.resolve(this._path))
	}

	get name() {
		return this.config.name || '<none>'
	}

	get options() {
		return this.config.options || {}
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
}

module.exports = PipelineConfig