import Message from '../../src/core/message.js'
import {MasterProtocol, WorkerProtocol, Event} from '../../src/core/protocol.js'

import {jest} from '@jest/globals'

describe('Protocol', () => {

	test('Master: message', async () => {
		const protocol = new MasterProtocol()

		const worker = {
			send: jest.fn()
		}

		const message = new Message()

		protocol.message(worker, message)

		expect(worker.send).toHaveBeenCalledTimes(1)
		expect(worker.send).toHaveBeenCalledWith({type: Event.MESSAGE, message})
	})

	test('Master: stop', async () => {
		const protocol = new MasterProtocol()

		const worker = {
			send: jest.fn()
		}

		protocol.stop(worker)

		expect(worker.send).toHaveBeenCalledTimes(1)
		expect(worker.send).toHaveBeenCalledWith({type: Event.STOP})
	})

	test('Worker: ready', async () => {
		const worker = {
			id: 'test'
		}

		const protocol = new WorkerProtocol(worker)

		const send = jest.spyOn(process, 'send')

		protocol.ready()

		expect(send).toHaveBeenCalledTimes(1)
		expect(send).toHaveBeenCalledWith({type: Event.READY, workerId: worker.id})

		send.mockRestore()
	})

	test('Worker: ready', async () => {
		const worker = {
			id: 'test'
		}

		const protocol = new WorkerProtocol(worker)

		const send = jest.spyOn(process, 'send')

		const metrics = {in: 5}
		protocol.stopped(metrics)

		expect(send).toHaveBeenCalledTimes(1)
		expect(send).toHaveBeenCalledWith({type: Event.STOPPED, workerId: worker.id, metrics})

		send.mockRestore()
	})

	test('Worker: shutdown', async () => {
		const worker = {
			id: 'test'
		}

		const protocol = new WorkerProtocol(worker)

		const send = jest.spyOn(process, 'send')

		const signal = 'SIGNAL'
		protocol.shutdown(signal)

		expect(send).toHaveBeenCalledTimes(1)
		expect(send).toHaveBeenCalledWith({type: Event.SHUTDOWN, workerId: worker.id, signal})

		send.mockRestore()
	})

	test('Worker: shutdown no signal', async () => {
		const worker = {
			id: 'test'
		}

		const protocol = new WorkerProtocol(worker)

		const send = jest.spyOn(process, 'send')

		protocol.shutdown()

		expect(send).toHaveBeenCalledTimes(1)
		expect(send).toHaveBeenCalledWith({type: Event.SHUTDOWN, workerId: worker.id, signal: 'SIGINT'})

		send.mockRestore()
	})

	test('Worker: broadcast', async () => {
		const worker = {
			id: 'test'
		}

		const protocol = new WorkerProtocol(worker)

		const send = jest.spyOn(process, 'send')

		const message = new Message()
		const pipelines = ['pipeline1', 'pipeline2']

		protocol.broadcast(pipelines, message)

		expect(send).toHaveBeenCalledTimes(1)
		expect(send).toHaveBeenCalledWith({type: Event.MESSAGE, mode: 'broadcast', pipelines, message, workerId: worker.id})

		send.mockRestore()
	})

	test('Worker: fanout', async () => {
		const worker = {
			id: 'test'
		}

		const protocol = new WorkerProtocol(worker)

		const send = jest.spyOn(process, 'send')

		const message = new Message()
		const pipelines = ['pipeline1', 'pipeline2']

		protocol.fanout(pipelines, message)

		expect(send).toHaveBeenCalledTimes(1)
		expect(send).toHaveBeenCalledWith({type: Event.MESSAGE, mode: 'fanout', pipelines, message, workerId: worker.id})

		send.mockRestore()
	})
})