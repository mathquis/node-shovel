import Prometheus from 'prom-client'
import Node from '../../src/node.js'
import {WorkerProtocol} from '../../src/protocol.js'
import PipelineConfig from '../../src/pipeline_config.js'
import Message from '../../src/message.js'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

describe('Api', () => {

	let pipelineConfig, protocol

	beforeAll(async () => {

		Logger.setLogLevel('emerg')

		pipelineConfig = new PipelineConfig()

		await pipelineConfig.load('./tests/assets/pipeline.yaml')

		protocol = new WorkerProtocol()
	})

	beforeEach(() => {
		Prometheus.register.clear()
	})

	test('start: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.START, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(node.isUp).toBeTruthy()
	})

	test('start: up', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.UP, listener)
		}
		await node.set(fn)

		await node.start()

		expect(node.isUp).toBeTruthy()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('stop: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.STOP, listener)
		}
		await node.set(fn)

		await node.start()
		await node.stop()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('stop: down', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.DOWN, listener)
		}
		await node.set(fn)

		await node.start()
		await node.stop()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('up: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.START, () => node.up())
			node.on(Node.Event.UP, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('down: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.DOWN, listener)
		}
		await node.set(fn)

		await node.start()
		await node.down()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('pause: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.PAUSE, listener)
		}
		await node.set(fn)

		await node.start()
		await node.pause()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('resume: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.RESUME, listener)
		}
		await node.set(fn)

		await node.start()
		await node.pause()
		await node.resume()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('in: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.IN, listener)
		}
		await node.set(fn)

		await node.start()
		await node.in(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('out: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.OUT, listener)
		}
		await node.set(fn)

		await node.start()
		await node.out(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('ack: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.ACK, listener)
		}
		await node.set(fn)

		await node.start()
		await node.ack(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('nack: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.NACK, listener)
		}
		await node.set(fn)

		await node.start()
		await node.nack(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('ignore: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.IGNORE, listener)
		}
		await node.set(fn)

		await node.start()
		await node.ignore(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('reject: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node.on(Node.Event.REJECT, listener)
		}
		await node.set(fn)

		await node.start()
		await node.reject(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('createMessage', async () => {
		expect.assertions(1)

		const node = new Node(pipelineConfig, protocol)

		let message

		const listener = jest.fn()
		const fn = node => {
			message = node.createMessage()
		}
		await node.set(fn)

		await node.start()

		expect(message).toBeInstanceOf(Message)
	})
})