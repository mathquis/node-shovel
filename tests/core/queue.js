import Prometheus from 'prom-client'
import Queue from '../../src/queue.js'
import {WorkerProtocol} from '../../src/protocol.js'
import PipelineConfig from '../../src/pipeline_config.js'
import Message from '../../src/message.js'
import Path from 'path'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

describe('Queue', () => {

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
		const node = new Queue(pipelineConfig, protocol)

		expect(node.options).toBe(pipelineConfig.queue)
		expect(node.includePaths).toEqual([pipelineConfig.path, Path.resolve('./src/queues/')])
	})

	test('evict: listener', async () => {
		expect.assertions(2)

		const node = new Queue(pipelineConfig, protocol)

		const message = node.createMessage()

		const listener = jest.fn()
		node.on('evict', listener)
		await node.start()
		await node.evict(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})
})