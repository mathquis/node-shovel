import Loadable from '../../src/loadable.js'
import PipelineConfig from '../../src/pipeline_config.js'
import Utils from '../../src/utils.js'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

describe('Loadable', () => {

	let pipelineConfig, protocol

	beforeAll(async () => {

		Logger.setLogLevel('emerg')

		pipelineConfig = new PipelineConfig()

		await pipelineConfig.load('./tests/assets/pipeline.yaml')
	})

	test('instance', async () => {
		const node = new Loadable(pipelineConfig)

		expect(node.util).toBe(Utils)
		expect(node.name).toEqual('')
		expect(node.options).toEqual({})
		expect(node.includePaths).toEqual([pipelineConfig.path])
	})

	test('load', async () => {
		expect.assertions(2)

		const node = new Loadable(pipelineConfig)

		const fnName = './fn.js'
		node.config.set('use', fnName)

		await node.load()

		expect(node.name).toEqual(fnName)
		expect(node.isLoaded).toBeTruthy()
	})
})