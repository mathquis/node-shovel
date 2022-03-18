import YAML from 'js-yaml'
import File from 'fs'
import Path from 'path'
import PipelineConfig from '../../src/pipeline_config.js'
import Logger from '../../src/logger.js'

import {jest} from '@jest/globals'

const pipeline = YAML.load(File.readFileSync('./tests/assets/pipeline.yaml'))

describe('PipelineConfig', () => {

	Logger.setLogLevel('emerg')

	test('empty', async () => {
		const conf = new PipelineConfig()

		expect(conf.name).toEqual('<none>')
		expect(conf.workers).toEqual(1)
		expect(conf.input).toEqual({})
		expect(conf.decoder).toEqual({})
		expect(conf.pipeline).toEqual({})
		expect(conf.encoder).toEqual({})
		expect(conf.queue).toEqual({})
		expect(conf.output).toEqual({})
		expect(conf.path).toEqual(Path.dirname(Path.resolve(process.cwd())))
	})

	test('set', async () => {
		const conf = new PipelineConfig()

		conf.set(pipeline)

		expect(conf.name).toBe(pipeline.name)
		expect(conf.workers).toBe(pipeline.workers)
		expect(conf.input).toBe(pipeline.input)
		expect(conf.decoder).toBe(pipeline.decoder)
		expect(conf.pipeline).toBe(pipeline.pipeline)
		expect(conf.encoder).toBe(pipeline.encoder)
		expect(conf.queue).toBe(pipeline.queue)
		expect(conf.output).toBe(pipeline.output)

		expect(conf.toJSON()).toBe(pipeline)
	})

	test('load: success', async () => {
		expect.assertions(9)

		const conf = new PipelineConfig()

		const filePath = './tests/assets/pipeline.yaml'

		await conf.load(filePath)

		expect(conf.name).toEqual(pipeline.name)
		expect(conf.workers).toEqual(pipeline.workers)
		expect(conf.input).toEqual(pipeline.input)
		expect(conf.decoder).toEqual(pipeline.decoder)
		expect(conf.pipeline).toEqual(pipeline.pipeline)
		expect(conf.encoder).toEqual(pipeline.encoder)
		expect(conf.queue).toEqual(pipeline.queue)
		expect(conf.output).toEqual(pipeline.output)
		expect(conf.path).toEqual(Path.dirname(Path.resolve(filePath)))
	})

	test('load: fail', async () => {
		expect.assertions(1)

		const conf = new PipelineConfig()

		try {
			await conf.load('error')
		} catch (err) {
			expect(err).toBeInstanceOf(Error)
		}
	})

	test('env: no default', async () => {

		const conf = new PipelineConfig()

		conf.set({
			input: {
				use: '${INPUT_NAME}'
			}
		})

		expect(conf.input.use).toEqual('')
	})

	test('env: default', async () => {
		const conf = new PipelineConfig()

		conf.set({
			input: {
				use: '${INPUT_NAME:default}',
				options: {
					boolean: true
				}
			}
		})

		expect(conf.input.use).toEqual('default')
		expect(conf.input.options.boolean).toStrictEqual(true)
	})

	test('env: value', async () => {
		process.env.INPUT_NAME = 'pass'

		const conf = new PipelineConfig()

		conf.set({
			input: {
				use: '${INPUT_NAME:default}'
			}
		})

		expect(conf.input.use).toEqual(process.env.INPUT_NAME)
		delete process.env.INPUT_NAME
	})

})