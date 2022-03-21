import File	from 'fs'
import Path	from 'path'
import Cluster from 'cluster'
import Level from 'level'
import LevelMem	from 'level-mem'
import {unpack, pack} from 'msgpackr'

const META_QUEUE_RETRIES	= 'queue-retries'
const META_QUEUE_STORED		= 'queue-stored'

export default node => {
	const batches = []
	const messages = []

	let db, flushTimeout, draining

	node
		.registerConfig({
			path: {
				doc: '',
				format: String,
				default: './.queues/'
			},
			flush_timeout: {
				doc: '',
				format: 'duration',
				default: 1000
			},
			retry_timeout: {
				doc: '',
				format: 'duration',
				default: 10000
			},
			batch_size: {
				doc: '',
				format: Number,
				default: 1000
			},
			queue_size: {
				doc: '',
				format: Number,
				default: Infinity
			},
			drain_timeout: {
				doc: '',
				format: 'duration',
				default: 60000
			},
			persistent: {
				doc: '',
				format: Boolean,
				default: false
			},
			sync: {
				doc: '',
				format: Boolean,
				default: false
			}
		})
		.onStart(async () => {
			const {path: dbPath, persistent, batch_size: batchSize, flush_timeout: timeout} = node.getConfig()

			// Check path
			await File.promises.mkdir(dbPath, {recursive: true})

			const Storage = persistent ? Level : LevelMem

			// Create database
			const queueName = `queued-${node.pipelineConfig.name}-${Cluster.worker.id}`
			const queuedDbPath = Path.resolve(dbPath + Path.sep + queueName)
			db = Storage(queuedDbPath, {
				valueEncoding: {
					type: 'msgpack',
					encode: pack,
					decode: unpack,
					buffer: true
				}
			})
			node.log.info('Opened queue at "%s" (persistent: %s, batch: %d, timeout: %d)', queuedDbPath, persistent, batchSize, timeout)

			await new Promise((resolve, reject) => {
				db
					.createValueStream()
					.on('data', obj => {
						const message = node.createMessage()
						message.fromObject(obj)
						messages.push(message)
						checkForFlush()
					})
					.on('error', reject)
					.on('end', () => resolve())
			})

			status()
		})
		.onStop(async () => {
			if ( db ) {
				await db.close()
			}
			stopFlushTimeout()
		})
		.onUp(async () => {
			flush()
		})
		.onDown(async () => {
			if ( draining ) {
				stopFlushTimeout()
				draining()
				return
			}
			// await new Promise((resolve, reject) => {
			// 	if ( keys.length === 0 ) {
			// 		resolve()
			// 		return
			// 	}
			// 	node.log.info('Draining (queued: %d, inflight: %d)', keys.length, inflight.length)
			// 	draining = resolve
			// 	const {drain_timeout: drainTimeout} = node.getConfig()
			// 	let timeout = setTimeout(() => {
			// 		reject(new Error('Queue drain timed out'))
			// 	}, drainTimeout)
			// })
			stopFlushTimeout()
		})
		.onPause(async () => {
			stopFlushTimeout()
		})
		.onResume(async () => {
			startFlushTimeout()
		})
		.onIn(async (message) => {
			if ( draining ) {
				node.nack(message)
				return
			}
			addToQueue(message)
			addToDb(message)
			node.ack(message)
			if ( checkForFlush() ) {
				flush()
			}
			if ( !flushTimeout ) {
				startFlushTimeout()
			}
		})
		.onNack((message) => {
			addToQueue(message)
			checkForFlush()
		})
		.onAck(async (message) => {
			if ( message.getHeader(META_QUEUE_STORED) ) {
				removeFromDb(message)
				return false
			}
			message.setHeader(META_QUEUE_STORED, true)
		})
		.onIgnore(async (message) => {
			removeFromDb(message)
		})
		.onReject(async (message) => {
			removeFromDb(message)
		})

	function status() {
		const {numQueued, numInflight} = stat()
		node.log.info('Status (queued: %d, batches: %d, inflight: %d)', numQueued, batches.length, numInflight)
	}

	function stat() {
		const numQueued = messages.length
		const numInflight = batches.reduce((count, batch) => count + batch.length, 0)

		return {numQueued, numInflight}
	}

	function addToQueue(message) {
		const {
			queue_size: queueSize
		} = node.getConfig()
		const {numQueued, numInflight} = stat()
		if ( numQueued + numInflight === queueSize ) {
			const evicted = messages.shift()
			if ( evicted ) {
				removeFromDb(evicted)
				node.evict(evicted)
			}
		}
		messages.push(message)
	}

	function addToDb(message) {
		const sync = node.getConfig('sync')
		return db.put(message.uuid, message.toObject(), {sync})
	}

	function removeFromDb(message) {
		const sync = node.getConfig('sync')
		return db.del(message.uuid, {sync})
	}

	function checkForFlush() {
		const {
			batch_size: batchSize
		} = node.getConfig()
		if ( messages.length >= batchSize ) {
			createBatch()
			return true
		}
		return false
	}

	function createBatch() {
		const {
			batch_size: batchSize
		} = node.getConfig()
		const batch = []
		messages.splice(0, batchSize).forEach(message => {
			message.incHeader(META_QUEUE_RETRIES)
			batch.push(message)
		})
		batches.push(batch)
		node.log.debug('Created batch (messages: %d, queued: %d)', batch.length, messages.length)
		handlePause()
		startFlushTimeout()
	}

	function handlePause() {
		const {
			queue_size: queueSize
		} = node.getConfig()
		const {numQueued, numInflight} = stat()
		if ( numQueued + numInflight >= queueSize ) {
			node.pause()
		} else {
			node.resume()
		}
	}

	async function flush() {
		stopFlushTimeout()
		handlePause()
		const {numQueued, numInflight} = stat()
		const batch = batches.shift()
		if ( batch ) {
			node.log.info('Flushing "%s" messages (batches: %d, queued: %d, inflight: %d)', batch.length, batches.length, numQueued, numInflight)
			batch.forEach(message => {
				node.out(message)
			})
		}
		startFlushTimeout()
	}

	function startFlushTimeout() {
		stopFlushTimeout()
		const {
			flush_timeout: timeout
		} = node.getConfig()
		flushTimeout = setTimeout(() => {
			if ( messages.length > 0 && batches.length === 0 ) {
				createBatch()
			}
			flush()
		}, timeout)
	}

	function stopFlushTimeout() {
		if ( !flushTimeout ) return
		clearTimeout(flushTimeout)
		flushTimeout = null
	}
}