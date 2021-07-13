const File		= require('fs')
const Path		= require('path')
const YAML		= require('js-yaml')
const Logger	= require('../logger')

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

	loadFn(fn) {
		let modulePath
		try {
			modulePath = require.resolve(fn)
		} catch (err) {
			this.log.debug('Function "%s" is not a Node.js module', fn)
		}
    try {
    	const searchPaths = [
    		modulePath, // NPM Module
    		Path.resolve(__dirname, '../codecs/' + fn), // Default codec
    		Path.resolve(this.path, fn) // User provided codec
    	]
    	const foundPath = searchPaths.filter(fnPath => !!fnPath).find(fnPath => {
    		this.log.debug('Checking function "%s" in path "%s"...', fn, fnPath)
    		return File.existsSync(fnPath) || File.existsSync(fnPath + '.js')
    	})
    	if ( !foundPath ) {
    		throw new Error(`No valid path available for function ${fn}`)
    	}
    	this.log.debug('Found function "%s" at path "%s"', fn, foundPath)
      return require(foundPath)
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