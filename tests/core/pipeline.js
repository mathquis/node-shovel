import Prometheus from 'prom-client'
import Pipeline from '../../src/core/pipeline.js'
import {WorkerProtocol} from '../../src/core/protocol.js'
import PipelineConfig from '../../src/core/pipeline_config.js'
import Message from '../../src/core/message.js'
import Path from 'path'
import Logger from '../../src/core/logger.js'

import {jest} from '@jest/globals'

describe('Pipeline', () => {

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
		const node = new Pipeline(pipelineConfig, protocol)

		expect(node.options).toBe(pipelineConfig.pipeline)
		expect(node.includePaths).toEqual([pipelineConfig.path, Path.resolve('./src/pipelines/')])
	})
})