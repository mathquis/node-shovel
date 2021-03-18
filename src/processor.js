const Stream        = require('stream')
const Path          = require('path')
const Prometheus    = require('prom-client')
const Convict       = require('convict')
const Logger        = require('./logger')

const registers = []

class Processor {
    constructor(name, options) {
        this.config = Convict({
            metrics: {
                labels: {
                    doc: '',
                    default: [],
                    format: Array
                }
            }
        })
        this.config.load(options)
        this.config.validate({allowed: 'strict'})

        this.name = name
        this.log = Logger.child({category: this.name})
        this.setupMetrics(options)
    }

    help() {
        return {
            input: this.input.help(),
            output: this.output.help()
        }
    }

    async start() {
        // Chain everything
        this.input
            .pipe(this.pipeline)
            .pipe(this.output)

        await this.output.start()
        await this.input.start()
    }

    async stop() {
        await this.input.stop()
        await this.output.stop()
    }

    setupMetrics(options) {
        const labelNames = ['kind', 'pipeline']

        this.globalMessage = new Prometheus.Counter({
            name: 'message_processed',
            help: 'Number of messages',
            labelNames
        })

        this.processingMessage = new Prometheus.Gauge({
            name: 'message_processing',
            help: 'Number of messages currently in the processing pipeline',
            labelNames
        })

        this.inputStatus = new Prometheus.Gauge({
            name: 'input_status',
            help: 'Status of the input connection',
            labelNames
        })

        this.inputMessage = new Prometheus.Counter({
            name: 'input_message',
            help: 'Number of input messages',
            labelNames
        })

        this.pipelineMessage = new Prometheus.Counter({
            name: 'pipeline_message',
            help: 'Number of pipeline messages',
            labelNames
        })

        this.outputMessage = new Prometheus.Counter({
            name: 'output_message',
            help: 'Number of output messages',
            labelNames
        })

        this.outputFlush = new Prometheus.Counter({
            name: 'output_flush',
            help: 'Number of output flushes',
            labelNames
        })
    }

    setupInput(input) {
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
            this.input = new inputClass(use, parserFn, options)
        } catch (err) {
            throw new Error(`Input error: ${err.message}`)
        }

        this.input
            .on('up', () => {
                this.inputStatus.set({pipeline: this.name, kind: 'up'}, 1)
            })
            .on('down', () => {
                this.inputStatus.set({pipeline: this.name, kind: 'up'}, 0)
            })
            .on('error', err => {
                this.log.error(`Input error: ${err.message}`)
                this.inputMessage.inc({pipeline: this.name, kind: 'error'})
            })
            .on('data', () => {
                this.processingMessage.inc()
                this.inputMessage.inc({pipeline: this.name, kind: 'received'})
            })
            .on('ack', message => {
                this.processingMessage.dec()
                this.inputMessage.inc({pipeline: this.name, kind: 'acked'})
                this.globalMessage.inc({pipeline: this.name})
            })
            .on('nack', message => {
                this.processingMessage.dec()
                this.inputMessage.inc({pipeline: this.name, kind: 'nacked'})
                this.globalMessage.inc({pipeline: this.name})
            })
            .on('reject', message => {
                this.processingMessage.dec()
                this.inputMessage.inc({pipeline: this.name, kind: 'rejected'})
                this.globalMessage.inc({pipeline: this.name})
            })
    }

    setupPipeline(pipeline) {
        const {use = '', options = {}} = pipeline || {}
        let pipelineFn
        try {
            pipelineFn = require(Path.resolve(process.cwd(), use))(options)
        } catch (err) {
            throw new Error(`Unknown pipeline "${use} (${err.message})`)
        }

        this.pipeline = new Stream.Transform({
            readableObjectMode: true,
            writableObjectMode: true,
            transform: async (message, enc, done) => {
                this.pipelineMessage.inc({pipeline: this.name, kind: 'received'})
                try {
                    await new Promise((resolve, reject) => {
                        pipelineFn(message, (err, messages) => {
                            if ( !messages ) {
                                if ( err ) {
                                    this.pipeline.emit('error', err)
                                    this.pipeline.emit('reject', message)
                                } else {
                                    this.pipeline.emit('ignore', message)
                                }
                            } else {
                                messages.forEach(message => {
                                    this.pipeline.push(message)
                                    this.pipeline.emit('ack', message)
                                })
                            }
                            resolve()
                        })
                    })
                } catch (err) {
                    this.input.nack(message)
                    this.pipeline.emit('error', err)
                }
                done()
            }
        })

        this.pipeline
            .on('error', err => {
                this.pipelineMessage.inc({pipeline: this.name, kind: 'error'})
                this.log.error(`Pipeline error: ${err.message}`)
            })
            .on('ack', message => {
                this.pipelineMessage.inc({pipeline: this.name, kind: 'acked'})
            })
            .on('nack', (err, message) => {
                this.input.nack(message)
                this.pipelineMessage.inc({pipeline: this.name, kind: 'nacked'})
            })
            .on('ignore', message => {
                this.input.ack(message)
                this.pipelineMessage.inc({pipeline: this.name, kind: 'ignored'})
            })
            .on('reject', message => {
                this.input.reject(message)
                this.pipelineMessage.inc({pipeline: this.name, kind: 'rejected'})
            })
    }

    setupOutput(output) {
        const {use = '', parser = {}, options = {}} = output || {}
        let outputClass
        try {
            outputClass = require('./outputs/' + use)
        } catch (err) {
            throw new Error(`Unknown output type "${use} (${err.message})`)
        }
        try {
            this.output = new outputClass(use, options)
        } catch (err) {
            throw new Error(`Output error: ${err.message}`)
        }

        this.output
            .on('error', err => {
                this.outputMessage.inc({pipeline: this.name, kind: 'error'})
                this.log.error(`Output error: ${err.message}`)
            })
            .on('incoming', message => {
                this.outputMessage.inc({pipeline: this.name, kind: 'received'})
            })
            .on('ack', message => {
                this.input.ack(message)
                this.outputMessage.inc({pipeline: this.name, kind: 'acked'})
            })
            .on('nack', (err, message) => {
                this.input.nack(message)
                this.outputMessage.inc({pipeline: this.name, kind: 'nacked'})
            })
            .on('reject', message => {
                this.input.reject(message)
                this.outputMessage.inc({pipeline: this.name, kind: 'rejected'})
            })
            .on('flush', () => {
                this.outputFlush.inc({pipeline: this.name})
            })
    }
}

module.exports = Processor