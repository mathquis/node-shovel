const Stream        = require('stream')
const EventEmitter  = require('events')
const Prometheus    = require('prom-client')

const registers = []

class Processor extends EventEmitter {
    constructor(name, input, pipeline, output) {
        super()
        this.name = name

        this.setupMetrics()

        this.setupInput(input)
        this.setupPipeline(pipeline)
        this.setupOutput(output)

        // Chain everything
        this.input
            .pipe(this.pipeline)
            .pipe(this.output)
    }

    async start() {
        await this.output.start()
        await this.input.start()
        this.emit('started')
    }

    async stop() {
        await this.input.stop()
        await this.output.stop()
        this.emit('stopped')
    }

    setupMetrics() {
        this.register = new Prometheus.Registry()
        registers.push(this.register)
        Prometheus.AggregatorRegistry.setRegistries(registers)

        this.register.setDefaultLabels({
            node: process.env['K8S_NODE_NAME'] || '',
            pod: process.env['K8S_POD_NAME'] || '',
            pipeline: this.name
        })

        this.globalMessage = new Prometheus.Counter({
            name: 'message_processed',
            help: 'Number of messages',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })

        this.processingMessage = new Prometheus.Gauge({
            name: 'message_processing',
            help: 'Number of messages currently in the processing pipeline',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })

        this.inputStatus = new Prometheus.Gauge({
            name: 'input_status',
            help: 'Status of the input connection',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })

        this.inputMessage = new Prometheus.Counter({
            name: 'input_message',
            help: 'Number of input messages',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })

        this.pipelineMessage = new Prometheus.Counter({
            name: 'pipeline_message',
            help: 'Number of pipeline messages',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })

        this.outputMessage = new Prometheus.Counter({
            name: 'output_message',
            help: 'Number of output messages',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })

        this.outputFlush = new Prometheus.Counter({
            name: 'output_flush',
            help: 'Number of output flushes',
            registers: [this.register],
            labelNames: ['node', 'pod', 'kind', 'pipeline']
        })
    }

    setupInput(input) {
        this.input = input
        this.input
            .on('up', () => {
                this.inputStatus.set({kind: 'up'}, 1)
            })
            .on('down', () => {
                this.inputStatus.set({kind: 'up'}, 0)
            })
            .on('error', err => {
                const error = new Error(`Input error: ${err.message}`)
                error.stack = err.stack
                this.inputMessage.inc({kind: 'error'})
                this.emit('error', error)
            })
            .on('data', () => {
                this.processingMessage.inc()
                this.inputMessage.inc({kind: 'received'})
            })
            .on('ack', message => {
                this.processingMessage.dec()
                this.inputMessage.inc({kind: 'acked'})
                this.globalMessage.inc()
            })
            .on('nack', message => {
                this.processingMessage.dec()
                this.inputMessage.inc({kind: 'nacked'})
                this.globalMessage.inc()
            })
            .on('reject', message => {
                this.processingMessage.dec()
                this.inputMessage.inc({kind: 'rejected'})
                this.globalMessage.inc()
                this.emit('reject', message)
            })
    }

    setupPipeline(pipeline) {
        this.pipeline = new Stream.Transform({
            readableObjectMode: true,
            writableObjectMode: true,
            transform: async (message, enc, done) => {
                this.pipelineMessage.inc({kind: 'received'})
                try {
                    await new Promise((resolve, reject) => {
                        pipeline(message, (err, messages) => {
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
                    this.pipeline.emit('nack', message)
                    log.error('Pipeline error: %s', err.message)
                    const error = new Error(`Pipeline error: ${err.message}`)
                    error.stack = err.stack
                    this.pipeline.emit('error', error)
                    return
                }
                done()
            }
        })

        this.pipeline
            .on('error', err => {
                const error = new Error(`Pipeline error: ${err.message}`)
                error.stack = err.stack
                this.pipelineMessage.inc({kind: 'error'})
                this.emit('error', error)
            })
            .on('ack', message => {
                this.pipelineMessage.inc({kind: 'acked'})
            })
            .on('nack', (err, message) => {
                this.input.nack(message)
                this.pipelineMessage.inc({kind: 'nacked'})
            })
            .on('ignore', message => {
                this.input.ack(message)
                this.pipelineMessage.inc({kind: 'ignored'})
            })
            .on('reject', message => {
                this.input.reject(message)
                this.pipelineMessage.inc({kind: 'rejected'})
            })
    }

    setupOutput(output) {
        this.output = output
        this.output
            .on('error', err => {
                const error = new Error(`Output error: ${err.message}`)
                error.stack = err.stack
                this.outputMessage.inc({kind: 'error'})
                this.emit('error', error)
            })
            .on('incoming', message => {
                this.outputMessage.inc({kind: 'received'})
            })
            .on('ack', message => {
                this.input.ack(message)
                this.outputMessage.inc({kind: 'acked'})
            })
            .on('nack', (err, message) => {
                this.input.nack(message)
                this.outputMessage.inc({kind: 'nacked'})
            })
            .on('reject', message => {
                this.input.reject(message)
                this.outputMessage.inc({kind: 'rejected'})
            })
            .on('flush', () => {
                this.outputFlush.inc()
                this.emit('flush')
            })
    }
}

module.exports = Processor