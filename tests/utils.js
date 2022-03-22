import Prometheus from 'prom-client'
import {WorkerProtocol} from '../src/core/protocol.js'
import PipelineConfig from '../src/core/pipeline_config.js'
import Decoder from '../src/core/decoder.js'
import Encoder from '../src/core/encoder.js'

import {jest} from '@jest/globals'

export function testDecoder(description, config, source, content) {
	test(description, async () => {
		expect.assertions(3)

		Prometheus.register.clear()

		const pipelineConfig = new PipelineConfig()
		pipelineConfig.set({
			decoder: config
		})

		const protocol = new WorkerProtocol()

		const node = new Decoder(pipelineConfig)
		await node.load()

		const message = node.createMessage()

		message.source = source

		let m
		const listener = jest.fn(message => m = message)
		node.on('out', listener)
		await node.in(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
		expect(m.content).toEqual(content)
	})
}

export function testEncoder(description, config, content, payload) {
	test(description, async () => {
		expect.assertions(3)

		Prometheus.register.clear()

		const pipelineConfig = new PipelineConfig()
		pipelineConfig.set({
			encoder: config
		})

		const protocol = new WorkerProtocol()

		const node = new Encoder(pipelineConfig)
		await node.load()

		const message = node.createMessage()

		message.content = content

		let m
		const listener = jest.fn(message => m = message)
		node.on('out', listener)
		await node.in(message)

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
		expect(m.payload).toEqual(payload)
	})
}