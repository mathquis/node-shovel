import Prometheus from 'prom-client'
import Input from '../../src/core/input.js'
import {WorkerProtocol} from '../../src/core/protocol.js'
import PipelineConfig from '../../src/core/pipeline_config.js'
import Message from '../../src/core/message.js'
import Path from 'path'
import Logger from '../../src/core/logger.js'

import {jest} from '@jest/globals'

describe('Input', () => {

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
		const node = new Input(pipelineConfig, protocol)

		expect(node.options).toBe(pipelineConfig.input)
		expect(node.includePaths).toEqual([pipelineConfig.path, Path.resolve('./src/inputs/')])
	})

	test('in: listener', async () => {
		expect.assertions(4)

		const node = new Input(pipelineConfig, protocol)

		const message = node.createMessage()

		const listenerIn = jest.fn()
		node.on('in', listenerIn)
		const listenerOut = jest.fn()
		node.on('out', listenerOut)
		await node.start()
		await node.in(message)

		expect(listenerIn).toHaveBeenCalledTimes(1)
		expect(listenerIn).toHaveBeenCalledWith(message)

		expect(listenerOut).toHaveBeenCalledTimes(1)
		expect(listenerOut).toHaveBeenCalledWith(message)
	})
})