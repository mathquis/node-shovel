META_QUEUE_RETRIES = 'queue_retries'

module.exports = node => {
	let flushTimeout, drained

	const queue = new Map()
	const inflight = new Map()

	node
		.registerConfig({
			queue_size: {
				doc: '',
				format: Number,
				default: 1000
			},
			flush_timeout: {
				doc: '',
				format: Number,
				default: 0
			},
			batch_size: {
				doc: '',
				format: Number,
				default: 1000
			},
			drain_timeout: {
				doc: '',
				format: Number,
				default: 60000
			}
		})
		.on('down', async () => {
			await new Promise((resolve, reject) => {
				if ( queue.size === 0 ) {
					resolve()
					return
				}
				node.log.info('Draining (queued: %d, inflight: %d)', queue.size, inflight.size)
				drained = resolve
				const {drain_timeout: drainTimeout} = node.getConfig()
				let timeout = setTimeout(() => {
					reject(new Error('Queue drain timed out'))
				}, drainTimeout)
			})
			stopFlushTimeout()
		})
		.on('in', (message) => {
			if ( drained ) {
				node.nack(message)
				return
			}
			pushToQueue(message)
			node.queued(message)
			if ( node.getConfig('flush_timeout') === 0 ) {
				sendDownstream(message)
			}
		})
		// .on('evicted', async (message) => {
		// 	removeFromQueue(message)
		// })
		.on('ack', async (message) => {
			removeFromInflight(message)
		})
		.on('nack', async (message) => {
			removeFromInflight(message, true)
		})
		.on('ignore', async (message) => {
			removeFromInflight(message)
		})
		.on('reject', async (message) => {
			removeFromInflight(message)
		})

	function sendDownstream(message) {
		const previousRetries = message.getMeta(META_QUEUE_RETRIES)
		const retries = typeof previousRetries !== 'number' ? 0 : previousRetries + 1
		message.setMeta(META_QUEUE_RETRIES, retries)
		pushToInflight(message)
		setImmediate(() => {
			node.out(message)
		})
	}

	function pushToQueue(message) {
		queue.set(message.uuid, message)
		// if ( queue.size > node.getConfig('queue_size') ) {
		// 	const [evicted] = queue.values()
		// 	node.evicted(evicted)
		// }
		if ( queue.size >= node.getConfig('queue_size') ) {
			node.log.debug('Full (queued: %s)', queue.size)
			node.pause()
		}
		if ( !flushTimeout ) {
			startFlushTimeout()
		}
		node.log.debug('Queued %s (queued: %d, inflight: %d)', message, queue.size, inflight.size)
	}

	function pushToInflight(message) {
		queue.delete(message.uuid)
		inflight.set(message.uuid, message)
		node.log.debug('Inflight %s (queued: %d, inflight: %d)', message, queue.size, inflight.size)
	}

	function removeFromQueue(message) {
		if ( queue.get(message.uuid) ) {
			queue.delete(message.uuid)
			node.log.debug('Remove from queue %s (queued: %d, inflight: %d)', message, queue.size, inflight.size)
		}
	}

	function removeFromInflight(message, requeue) {
		if ( inflight.get(message.uuid) ) {
			inflight.delete(message.uuid)
			if ( requeue ) {
				queue.set(message.uuid, message)
			}
			node.log.debug('Remove from inflight %s (queued: %d, inflight: %d)', message, queue.size, inflight.size)
		}

		if ( inflight.size === 0 ) {
			if ( queue.size === 0 ) {
				if ( node.getConfig('flush_timeout') > 0 ) {
					node.log.info('Empty')
				}
				if ( drained ) {
					drained()
					drained = null
					return
				}
				setImmediate(() => node.resume())
			}
		}
	}

	function flush(batchSize) {
		if ( queue.size === 0 ) {
			stopFlushTimeout()
			return
		}
		let i = 0
		for ( i = 0 ; i < batchSize ; i++ ) {
			const [message] = queue.values()
			if ( !message ) {
				break
			}
			sendDownstream(message)
		}
		node.log.info('Flushed "%s" messages (queue: %d, inflight: %d)', i, queue.size, inflight.size)
	}

	function startFlushTimeout() {
		if ( node.getConfig('flush_timeout') === 0 ) return
		stopFlushTimeout()
		flushTimeout = setTimeout(() => {
			const {batch_size: batchSize} = node.getConfig()
			flush(batchSize)
			startFlushTimeout()
		}, node.getConfig('flush_timeout'))
	}

	function stopFlushTimeout() {
		if ( !flushTimeout ) return
		clearTimeout(flushTimeout)
		flushTimeout = null
	}
}