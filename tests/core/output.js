import Prometheus from 'prom-client'
import Output from '../../src/output.js'
import {WorkerProtocol} from '../../src/protocol.js'
import PipelineConfig from '../../src/pipeline_config.js'
import Message from '../../src/message.js'
import Path from 'path'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

describe('Output', () => {

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
		const node = new Output(pipelineConfig, protocol)

		expect(node.options).toBe(pipelineConfig.output)
		expect(node.includePaths).toEqual([pipelineConfig.path, Path.resolve('./src/outputs/')])
	})

	test('ack: listener', async () => {
		expect.assertions(4)

		const node = new Output(pipelineConfig, protocol)

		const message = node.createMessage()

		const listenerAck = jest.fn()
		node.on('ack', listenerAck)
		const listenerOut = jest.fn()
		node.on('out', listenerOut)
		await node.start()
		await node.ack(message)

		expect(listenerAck).toHaveBeenCalledTimes(1)
		expect(listenerAck).toHaveBeenCalledWith(message)

		expect(listenerOut).toHaveBeenCalledTimes(1)
		expect(listenerOut).toHaveBeenCalledWith(message)
	})

	test('nack: listener', async () => {
		expect.assertions(4)

		const node = new Output(pipelineConfig, protocol)

		const message = node.createMessage()

		const listenerNack = jest.fn()
		node.on('nack', listenerNack)
		const listenerOut = jest.fn()
		node.on('out', listenerOut)
		await node.start()
		await node.nack(message)

		expect(listenerNack).toHaveBeenCalledTimes(1)
		expect(listenerNack).toHaveBeenCalledWith(message)

		expect(listenerOut).toHaveBeenCalledTimes(1)
		expect(listenerOut).toHaveBeenCalledWith(message)
	})

	test('ignore: listener', async () => {
		expect.assertions(4)

		const node = new Output(pipelineConfig, protocol)

		const message = node.createMessage()

		const listenerIgnore = jest.fn()
		node.on('ignore', listenerIgnore)
		const listenerOut = jest.fn()
		node.on('out', listenerOut)
		await node.start()
		await node.ignore(message)

		expect(listenerIgnore).toHaveBeenCalledTimes(1)
		expect(listenerIgnore).toHaveBeenCalledWith(message)

		expect(listenerOut).toHaveBeenCalledTimes(1)
		expect(listenerOut).toHaveBeenCalledWith(message)
	})

	test('reject: listener', async () => {
		expect.assertions(4)

		const node = new Output(pipelineConfig, protocol)

		const message = node.createMessage()

		const listenerReject = jest.fn()
		node.on('reject', listenerReject)
		const listenerOut = jest.fn()
		node.on('out', listenerOut)
		await node.start()
		await node.reject(message)

		expect(listenerReject).toHaveBeenCalledTimes(1)
		expect(listenerReject).toHaveBeenCalledWith(message)

		expect(listenerOut).toHaveBeenCalledTimes(1)
		expect(listenerOut).toHaveBeenCalledWith(message)
	})
})