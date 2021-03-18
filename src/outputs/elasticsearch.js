const File			= require('fs')
const Path			= require('path')
const Compile		= require('string-template/compile')
const {Client}		= require('@elastic/elasticsearch')
const OutputNode	= require('../output')

const META_INDEX_TEMPLATE = 'elasticsearch_index'

class ElasticsearchOutput extends OutputNode {
	constructor(options) {
		super(options)

		let ca
		if ( this.getConfig('ssl.ca') ) {
			const caPath = Path.resolve(process.cwd(), this.getConfig('ssl.ca'))
			ca = File.readFileSync(caPath)
		}

		const opts = {
			node: `${this.getConfig('scheme')}://${this.getConfig('host')}:${this.getConfig('port')}`,
			auth: {
				username: this.getConfig('username'),
				password: this.getConfig('password')
			},
			ssl: {
				ca,
				rejectUnauthorized: this.getConfig('ssl.reject_unauthorized')
			}
		}

		this.client			= new Client(opts)
		this.indexTemplates	= new Map()
		this.queue			= []
		this.flushTimeout	= null

		this.compileIndexTemplate(this.indexShardName)
	}

	get configSchema() {
		return {
			scheme: {
				doc: '',
				default: 'https',
				arg: 'es-scheme',
				env: 'ELASTICSEARCH_SCHEME'
			},
			host: {
				doc: '',
				default: 'localhost',
				arg: 'es-host',
				env: 'ELASTICSEARCH_HOST'
			},
			port: {
				doc: '',
				format: 'port',
				default: 9200,
				arg: 'es-port',
				env: 'ELASTICSEARCH_PORT'
			},
			username: {
				doc: '',
				default: '',
				env: 'ELASTICSEARCH_USERNAME'
			},
			password: {
				doc: '',
				default: '',
				sensitive: true,
				env: 'ELASTICSEARCH_PASSWORD'
			},
			ssl: {
				ca: {
					doc: '',
					default: '',
					arg: 'es-ca-cert',
					env: 'ELASTICSEARCH_CA_CERT'
				},
				reject_unauthorized: {
					doc: '',
					default: true,
					format: Boolean,
					arg: 'es-reject-unauthorized',
					env: 'ELASTICSEARCH_SSL_VERIFY'
				}
			},
			queue_size: {
				doc: '',
				default: 1000,
				format: Number,
				arg: 'es-queue-size'
			},
			queue_timeout: {
				doc: '',
				default: 10000,
				format: Number,
				arg: 'es-queue-timeout'
			},
			fail_timeout: {
				doc: '',
				default: 5000,
				format: Number,
				arg: 'es-fail-timeout'
			},
			index_name: {
				doc: '',
				default: 'message',
				arg: 'es-index-name',
				env: 'ELASTICSEARCH_INDEX_NAME'
			},
			index_shard: {
				doc: '',
				default: '{YYYY}',
				arg: 'es-index-shard',
				env: 'ELASTICSEARCH_INDEX_SHARD'
			},
			template: {
				doc: '',
				default: '',
				arg: 'es-template'
			}
		}
	}

	get indexShardName() {
		const indexShard = this.getConfig('index_shard')
		return this.getConfig('index_name') + ( indexShard ? `-${indexShard}` : '' )
	}

	async setupTemplate() {
		const templateFile = this.getConfig('template')
		if ( templateFile ) {
			const templatePath = Path.resolve(process.cwd(), templateFile)
			this.log.debug('Setting up template "%s"...', templatePath)

			let tpl
			try {
				tpl = require(templatePath)
				if ( typeof tpl === 'function' ) {
					tpl = tpl(this.config)
				}
			} catch (err) {
				this.emit('error', new Error(`Template "${templatePath}" not found: ${err.message}`))
				return
			}

			try {
				await this.client.indices.getTemplate({
					name: tpl.name
				})
				this.log.info('Template already created')
				return
			} catch (err) {
				this.log.warn('Template "%s" not created', tpl.name)
			}

			try {
				this.log.debug('Creating template...')
				await this.client.indices.putTemplate({
					name: tpl.name,
					body: tpl.template
				})
				this.log.info('Created template')
			} catch (err) {
				this.emit('error', new Error(`Unable to create template: ${err.message}`))
			}
		}
	}

	async start() {
		this.log.debug('Starting...')
		await this.setupTemplate()
		this.log.info('Connected')
		this.startFlushTimeout()
		await super.start()
	}

	async stop() {
		this.log.debug('Stopping...')
		this.stopFlushTimeout()
		await this.flush()
		await super.stop()
	}

	compileIndexTemplate(template) {
		this.indexTemplates.set(template, Compile(template))
	}

	formatIndexName(message) {
		const index = message.getMeta(META_INDEX_TEMPLATE) || this.indexShardName
		const indexTemplate = this.indexTemplates.get(index)
		if ( !indexTemplate ) {
			throw new Error(`Unknown index "${index}"`)
		}
		const {date} = message
		return indexTemplate({
			YYYY: date.getFullYear(),
			YY: date.getYear(),
			MM: date.getUTCMonth().toString().padStart(2, '0'),
			M: date.getUTCMonth(),
			DD: date.getUTCDate().toString().padStart(2, '0'),
			D: date.getUTCDate()
		}).toLowerCase()
	}

	startFlushTimeout() {
		this.stopFlushTimeout()
		setTimeout(() => {
			this.flush()
		}, this.getConfig('queue_timeout'))
		this.log.debug('Next flush in %dms', this.getConfig('queue_timeout'))
	}

	stopFlushTimeout() {
		if ( !this.flushTimeout ) return
		clearTimeout(this.flushTimeout)
		this.flushTimeout = null
	}

	async write(message) {
		this.queue.push(message)
		await super.write(message)
		if ( this.queue.length >= this.getConfig('queue_size') ) {
			await this.flush()
		}
	}

	async flush() {
		this.stopFlushTimeout()

		if ( this.queue.length > 0 ) {

			const st = (new Date()).getTime()

			this.log.debug('Flushing %d messages...', this.queue.length)

			// Get the queue messages
			const messages = this.queue

			// Empty the queue so new messages can start coming in
			this.queue = []

			// Index the messages
			let response
			try {
				response = await this.client.bulk({
					_source: ['uuid'],
					body: messages.flatMap(message => {
						return [
							{
								index: {
									_index: this.formatIndexName(message),
									_id: message.id
								}
							},
							message.content
						]
					})
				}, {
					filterPath: 'items.*.error,items.*._id'
				})
			} catch (err) {
				this.emit('error', err)
				// Notify messages processing
				setTimeout(() => {
					messages.forEach(message => {
						this.nack(message)
					})
				}, this.getConfig('fail_timeout'))
				return
			}

			const et = (new Date()).getTime()

			this.log.info('Flushed %d messages in %fms', messages.length, et-st)

			await super.flush()

			// Get messages in error
			const errorIds = new Map()
			if ( response.errors ) {
				response.items.forEach(item => {
					errorIds.set(item._id, true)
				})
			}

			// Notify messages processing
			messages.forEach(message => {
				if ( errorIds.get(message.id) ) {
					this.nack(message)
				} else {
					this.ack(message)
				}
			})

		} else {
			this.log.debug('Nothing to flush')
		}

		this.startFlushTimeout()
	}
}

module.exports = ElasticsearchOutput