const Stream = require('stream')
const Convict = require('convict')
const Logger = require('./logger')

class Node extends Stream.PassThrough {
	constructor(options) {
		options || (options = {})

		super({
			...options.stream,
			objectMode: true
		})

		this.config = Convict(this.configSchema || {})
		this.config.load(options)
		this.config.validate({allowed: 'strict'})

		this.isStarted	= false

		const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
			return $1 + '-' + $2.toLowerCase()
		}).toLowerCase()

		this.log = Logger.child({category: type, worker: process.pid})

		this.log.debug('%O', this.config.getProperties())
	}

	async start() {
		this.isStarted = true
		this.log.info('Started')
	}

	async stop() {
		this.isStarted = false
		this.log.info('Stopped')
	}

	getConfig(key) {
		return this.config.get(key)
	}

	ack(message) {
		this.log.debug('Acked (id: %s)', message.id)
		this.emit('ack', message)
	}

	nack(message) {
		this.log.warn('Nacked (id: %s)', message.id)
		this.emit('nack', message)
	}

	reject(message) {
		this.log.warn('Rejected (id: %s)', message.id)
		this.emit('reject', message)
	}
}

module.exports = Node