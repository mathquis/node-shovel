import Prometheus from 'prom-client'
import Node from '../../src/node.js'
import {WorkerProtocol} from '../../src/protocol.js'
import PipelineConfig from '../../src/pipeline_config.js'
import Message from '../../src/message.js'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

describe('Node', () => {

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

	test('instance', async () => {
		const node = new Node(pipelineConfig, protocol)

		expect(node.isStarted).toBeFalsy()
		expect(node.isUp).toBeFalsy()
		expect(node.isPaused).toBeFalsy()

		expect(node.name).toEqual('')
		expect(node.options).toEqual({})
		expect(node.includePaths).toEqual([pipelineConfig.path])

		// expect(node.protocol).toBeInstanceOf(WorkerProtocol)
		// expect(node.protocol).toBe(protocol)

		expect(node.defaultLabels).toEqual({pipeline: pipelineConfig.name})

		expect(node.createMessage()).toBeInstanceOf(Message)

		const mockEmit = jest.spyOn(process, 'emit').mockImplementation(() => {});
		node.shutdown()
		expect(mockEmit).toHaveBeenCalledTimes(1)
		mockEmit.mockRestore()

	})

	test('load', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const fnName = './fn.js'
		node.config.set('use', fnName)

		await node.load()

		expect(node.name).toEqual(fnName)
		expect(node.isLoaded).toBeTruthy()
	})

	test('load: invalid', async () => {
		expect.assertions(1)

		const node = new Node(pipelineConfig, protocol)

		const fnName = './fn-fail.js'
		node.config.set('use', fnName)

		try {
			await node.load()
		} catch (err) {
			expect(err).toBeInstanceOf(Error)
		}
	})

	test('start: listener', async () => {
		expect.assertions(5)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.START, listener)
		await node.start()

		expect(node.isStarted).toBeTruthy()
		expect(listener).toHaveBeenCalledTimes(1)
		expect(node.isUp).toBeTruthy()
		expect(node.isPaused).toBeFalsy()

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('start: up', async () => {
		expect.assertions(4)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.UP, listener)

		await node.start()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeTruthy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).toHaveBeenCalledTimes(1)
	})
	test('stop: listener', async () => {
		expect.assertions(6)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.STOP, listener)
		await node.start()
		await node.stop()

		expect(node.isStarted).toBeFalsy()
		expect(node.isUp).toBeFalsy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)

		await node.stop()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('stop: down', async () => {
		expect.assertions(6)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.DOWN, listener)
		await node.start()
		await node.stop()

		expect(node.isStarted).toBeFalsy()
		expect(node.isUp).toBeFalsy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)

		await node.stop()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('up: listener', async () => {
		expect.assertions(6)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.UP, listener)
		await node.start()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeTruthy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)

		await node.up()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('down: listener', async () => {
		expect.assertions(6)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.DOWN, listener)
		await node.start()
		await node.down()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeFalsy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)

		await node.down()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('pause: listener', async () => {
		expect.assertions(6)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.PAUSE, listener)
		await node.start()
		await node.pause()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeTruthy()
		expect(node.isPaused).toBeTruthy()
		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)

		await node.pause()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('resume: listener', async () => {
		expect.assertions(6)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.RESUME, listener)
		await node.start()
		await node.pause()
		await node.resume()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeTruthy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)

		await node.resume()

		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('pause: not up', async () => {
		expect.assertions(4)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.PAUSE, listener)
		await node.start()
		await node.down()
		await node.pause()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeFalsy()
		expect(node.isPaused).toBeFalsy()
		expect(listener).not.toHaveBeenCalled()
	})

	test('resume: not up', async () => {
		expect.assertions(4)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		node.on(Node.Event.RESUME, listener)
		await node.start()
		await node.pause()
		await node.down()
		await node.resume()

		expect(node.isStarted).toBeTruthy()
		expect(node.isUp).toBeFalsy()
		expect(node.isPaused).toBeTruthy()
		expect(listener).not.toHaveBeenCalled()
	})

	test('in: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		node.on(Node.Event.IN, listener)
		await node.start()
		await node.in(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('in: throw', async () => {
		expect.assertions(4)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const err = new Error()

		const thrower = async () => {
			throw err
		}
		const listener = jest.fn()
		const rejecter = jest.fn()
		node.on(Node.Event.IN, thrower)
		node.on(Node.Event.ERROR, listener)
		node.on(Node.Event.REJECT, rejecter)
		await node.start()
		await node.in(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(err, message)

		expect(rejecter).toHaveBeenCalledTimes(1)
		expect(rejecter).toHaveBeenCalledWith(message)
	})

	test('out: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		node.on(Node.Event.OUT, listener)
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
		node.on(Node.Event.ACK, listener)
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
		node.on(Node.Event.NACK, listener)
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
		node.on(Node.Event.IGNORE, listener)
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
		node.on(Node.Event.REJECT, listener)
		await node.start()
		await node.reject(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('error: listener', async () => {
		expect.assertions(4)

		const node = new Node(pipelineConfig, protocol)

		const error = new Error()

		const listener = jest.fn()
		node.on(Node.Event.ERROR, listener)
		await node.error(error)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(error, undefined)

		const errorWithOrigin = new Error()
		const message = new Message()

		await node.error(errorWithOrigin, message)

		expect(listener).toHaveBeenCalledTimes(2)
		expect(listener).toHaveBeenCalledWith(errorWithOrigin, message)
	})

	test('pipe', async () => {
		expect.assertions(24)

		const node1 = new Node(pipelineConfig, protocol)

		Prometheus.register.clear()

		const node2 = new Node(pipelineConfig, protocol)

		const listenerIn = jest.fn()
		node2.on(Node.Event.IN, listenerIn)

		const listenerAck = jest.fn()
		node1.on(Node.Event.ACK, listenerAck)

		const listenerNack = jest.fn()
		node1.on(Node.Event.NACK, listenerNack)

		const listenerIgnore = jest.fn()
		node1.on(Node.Event.IGNORE, listenerIgnore)

		const listenerReject = jest.fn()
		node1.on(Node.Event.REJECT, listenerReject)

		const listenerPause = jest.fn()
		node1.on(Node.Event.PAUSE, listenerPause)

		const listenerResume = jest.fn()
		node1.on(Node.Event.RESUME, listenerResume)

		const returnValue = node1.pipe(node2)

		expect(returnValue).toBe(node2)

		const message = node1.createMessage()

		await node1.start()
		await node2.start()
		await node1.out(message)

		expect(listenerIn).toHaveBeenCalledTimes(1)
		expect(listenerIn).toHaveBeenCalledWith(message)

		await node2.ack(message)

		expect(listenerAck).toHaveBeenCalledTimes(1)
		expect(listenerAck).toHaveBeenCalledWith(message)

		await node2.nack(message)

		expect(listenerNack).toHaveBeenCalledTimes(1)
		expect(listenerNack).toHaveBeenCalledWith(message)

		await node2.ignore(message)

		expect(listenerIgnore).toHaveBeenCalledTimes(1)
		expect(listenerIgnore).toHaveBeenCalledWith(message)

		await node2.reject(message)

		expect(listenerReject).toHaveBeenCalledTimes(1)
		expect(listenerReject).toHaveBeenCalledWith(message)

		await node2.pause()

		expect(listenerPause).toHaveBeenCalledTimes(1)

		await node2.resume()

		expect(listenerIn).toHaveBeenCalledTimes(1)
		expect(listenerIn).toHaveBeenCalledWith(message)

		expect(listenerAck).toHaveBeenCalledTimes(1)
		expect(listenerAck).toHaveBeenCalledWith(message)

		expect(listenerNack).toHaveBeenCalledTimes(1)
		expect(listenerNack).toHaveBeenCalledWith(message)

		expect(listenerIgnore).toHaveBeenCalledTimes(1)
		expect(listenerIgnore).toHaveBeenCalledWith(message)

		expect(listenerReject).toHaveBeenCalledTimes(1)
		expect(listenerReject).toHaveBeenCalledWith(message)

		expect(listenerPause).toHaveBeenCalledTimes(1)

		expect(listenerResume).toHaveBeenCalledTimes(1)
	})
})