import Cluster from 'cluster'
import Logger from './logger.js'

export const Event = {
	READY: 'pipeline:ready',
	STOP: 'pipeline:stop',
	STOPPED: 'pipeline:stopped',
	MESSAGE: 'pipeline:message',
	SHUTDOWN: 'shutdown'
}

export class MasterProtocol {
	constructor() {
	   this.log = Logger.child({category: 'master-protocol'})
	}

	message(worker, message) {
		this.log.debug('Sending message (worker: %d)', worker.id)
		worker.send({
			type: Event.MESSAGE,
			message
		})
	}
	stop(worker) {
		this.log.debug('Sending stop (worker: %d)', worker.id)
		worker.send({
			type: Event.STOP
		})
	}
	stdin(line) {
		worker.send({
			type: Event.STDIN,
			line: line
		})
	}
}

export class WorkerProtocol {
	constructor() {
	   this.log = Logger.child({category: 'worker-protocol'})
	}

	stopped(metrics) {
		this.log.debug('Sending stopped')
		process.send({
			type: Event.STOPPED,
			workerId: Cluster.worker.id,
			metrics
		})
	}
	ready() {
		this.log.debug('Ready')
		process.send({
			type: Event.READY,
			workerId: Cluster.worker.id
		})
	}
	shutdown(signal = 'SIGINT') {
		this.log.debug('Shutdown (signal: %s)', signal)
		process.send({
			type: Event.SHUTDOWN,
			workerId: Cluster.worker.id,
			signal
		})
	}
	broadcast(pipelines, message) {
		this.log.debug('Sending message to pipelines "%s" (mode: broadcast)', pipelines.join(','))
		process.send({
			type: Event.MESSAGE,
			workerId: Cluster.worker.id,
			pipelines,
			mode: 'broadcast',
			message
		})
	}
	fanout(pipelines, message) {
		this.log.debug('Sending message to pipelines "%s" (mode: fanout)', pipelines.join(','))
		process.send({
			type: Event.MESSAGE,
			workerId: Cluster.worker.id,
			pipelines,
			mode: 'fanout',
			message
		})
	}
}