const AMQP		= require('amqplib')
const InputNode	= require('../input')
const Message	= require('../message')

const META_AMQP = 'amqp'
let consumers = 0

class AmqpInput extends InputNode {
	get configSchema() {
		return {
			host: {
				doc: '',
				default: 'localhost',
				arg: 'amqp-host',
				env: 'RABBITMQ_HOST'
			},
			port: {
				doc: '',
				format: 'port',
				default: 5672,
				arg: 'amqp-port',
				env: 'RABBITMQ_PORT'
			},
			vhost: {
				doc: '',
				default: '/',
				arg: 'amqp-vhost',
				env: 'RABBITMQ_VHOST'
			},
			username: {
				doc: '',
				default: '',
				env: 'RABBITMQ_USERNAME'
			},
			password: {
				doc: '',
				default: '',
				sensitive: true,
				env: 'RABBITMQ_PASSWORD'
			},
			exchange_name: {
				doc: '',
				default: 'exchange',
				arg: 'amqp-exchange-name'
			},
			exchange_type: {
				doc: '',
				default: 'topic',
				arg: 'amqp-exchange-type'
			},
			queue_name: {
				doc: '',
				default: 'indexer',
				arg: 'amqp-queue-name'
			},
			bind_pattern: {
				doc: '',
				default: '#',
				arg: 'amqp-bind-pattern'
			},
			queue_size: {
				doc: '',
				default: 1000,
				arg: 'amqp-queue-size'
			},
			reconnect_after_ms: {
				doc: '',
				default: 5000,
				arg: 'amqp-reconnect-after-ms'
			}
		}
	}

	get queueSize() {
		const queueSize = parseInt(this.getConfig('queue_size'))
		if ( !queueSize || isNaN(queueSize) ) {
			return 500
		}
		return queueSize
	}

	get reconnectAfterMs() {
		const reconnectAfterMs = parseInt(this.getConfig('reconnect_after_ms'))
		if ( !reconnectAfterMs || isNaN(reconnectAfterMs) ) {
			return 500
		}
		return reconnectAfterMs
	}

	async connect() {
		try {
			this.log.debug('Connecting...')

			this.clearReconnectTimeout()

			// Connect to AMQP
			const {host: hostname, port, vhost, username, password} = this

			const opts = {
				hostname  : this.getConfig('host'),
				port      : this.getConfig('port'),
				vhost     : this.getConfig('vhost'),
				username  : this.getConfig('username'),
				password  : this.getConfig('password')
			}

			this.connection = await AMQP.connect(opts)

			this.connection
				.on('close', () => {
					this.log.warn('Connection closed')
					this.reconnect()
				})
				.on('error', err => {
					this.emit('error', err)
					this.reconnect()
				})

			await this.onConnect()
		} catch (err) {
			this.emit('error', err)
			this.reconnect()
		}
	}

	async reconnect() {
		this.log.debug('Reconnecting in %d...', this.reconnectAfterMs)
		this.connection = null
		this.channel = null
		this.emit('down')
		this.clearReconnectTimeout()
		this.reconnectTimeout = setTimeout(() => {
			this.connect()
		}, this.reconnectAfterMs)
	}

	clearReconnectTimeout() {
		if ( !this.reconnectTimeout ) return
		clearTimeout(this.reconnectTimeout)
		this.reconnectTimeout = null
	}

	async onConnect() {
		this.log.info('Connected')

		this.channel = await this.connection.createChannel()

		await this.channel.prefetch(this.queueSize)

		const queue = await this.channel.assertQueue(this.getConfig('queue_name'), {
			durable: true
		})

		await this.channel.bindQueue(this.getConfig('queue_name'), this.getConfig('exchange_name'), this.getConfig('bind_pattern'))

		this.channel
			.on('close', () => {
				this.log.info('Channel closed')
				this.channel = null
			})
			.on('error', err => {
				this.emit('error', err)
				this.channel = null
				setTimeout(() => {
					this.onConnect()
				}, this.reconnectAfterMs)
			})

		this.channel.consume(this.getConfig('queue_name'), async msg => {
			try {
				const content = await this.parse(msg)
				if ( !content ) {
					this.channel.nack(msg, false, false)
					this.emit('reject', new Error(`Unable to parse message`), msg)
					return
				}
				const message = new Message(content)
				message.setMeta(META_AMQP, msg.fields)
				this.push(message)
			} catch (err) {
				this.emit('error', err)
				const message = new Message(msg)
				message.setMeta(META_AMQP, msg.fields)
				this.nack(message)
			}
		}, {
			consumerTag: this.consumerTag,
			noAck: false
		})

		this.emit('up')
	}

	async start() {
		this.log.debug('Starting...')
		await this.connect()
		await super.start()
	}

	async stop() {
		this.log.debug('Stopping...')
		if ( this.channel ) {
			await this.channel.cancel(this.consumerTag)
		}
		await super.stop()
	}

	ack(message) {
		if ( !this.channel ) return
		const fields = message.getMeta(META_AMQP)
		if ( fields ) {
			this.channel.ack({fields})
			super.ack(message)
		}
	}

	nack(message) {
		if ( !this.channel ) return
		const fields = message.getMeta(META_AMQP)
		if ( fields ) {
			this.channel.nack({fields}, false, true)
			super.nack(message)
		}
	}

	reject(message) {
		if ( !this.channel ) return
		const fields = message.getMeta(META_AMQP)
		if ( fields ) {
			this.channel.nack({fields}, false, false)
			super.reject(message)
		}
	}
}

module.exports = AmqpInput