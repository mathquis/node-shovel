const Path		= require('path')
const Logger 	= require('./logger')
const Processor	= require('./processor')

class Pipeline {
	constructor(options) {
		this.name = options.name

		this.log = Logger.child({category: 'pipeline'})

		const input		= this.createInput(options.input || {})
		const pipeline	= this.createPipeline(options.pipeline || {})
		const output	= this.createOutput(options.output || {})

		this.processor = new Processor(
			this.name,
			input,
			pipeline,
			output
		)

		this.processor
			.on('error', err => this.log.error(err.message))
	}

	async start() {
		await this.processor.start()
	}

	async stop() {
		await this.processor.stop()
	}

	createInput(input) {
		const {use = '', parser = {}, options = {}} = input || {}
		let inputClass
		try {
			inputClass = require('./inputs/' + use)
		} catch (err) {
			throw new Error(`Unknown input type "${use} (${err.message})`)
		}
		let parserFn
		try {
			parserFn = require(Path.resolve(process.cwd(), parser.use))(parser.options)
		} catch (err) {
			throw new Error(`Unknown parser "${parser.use} (${err.message})`)
		}
		try {
			return new inputClass(parserFn, input.options)
		} catch (err) {
			throw new Error(`Input error: ${err.message}`)
		}
	}

	createPipeline(pipeline) {
		const {use = '', options = {}} = pipeline || {}
		try {
			return require(Path.resolve(process.cwd(), use))(options)
		} catch (err) {
			throw new Error(`Unknown pipeline "${use} (${err.message})`)
		}
	}

	createOutput(output) {
		const {use = '', parser = {}, options = {}} = output || {}
		let outputClass
		try {
			outputClass = require('./outputs/' + output.use)
		} catch (err) {
			throw new Error(`Unknown output type "${use} (${err.message})`)
		}
		try {
			return new outputClass(output.options)
		} catch (err) {
			throw new Error(`Output error: ${err.message}`)
		}
	}
}

module.exports = Pipeline