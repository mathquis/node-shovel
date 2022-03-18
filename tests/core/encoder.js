import Prometheus from 'prom-client'
import Encoder from '../../src/encoder.js'
import {WorkerProtocol} from '../../src/protocol.js'
import PipelineConfig from '../../src/pipeline_config.js'
import Message from '../../src/message.js'
import Path from 'path'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

describe('Encoder', () => {

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
		const node = new Encoder(pipelineConfig, protocol)

		expect(node.options).toBe(pipelineConfig.encoder)
		expect(node.includePaths).toEqual([pipelineConfig.path, Path.resolve('./src/encoders/')])
	})
})