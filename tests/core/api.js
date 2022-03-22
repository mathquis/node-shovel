import Prometheus from 'prom-client'
import Node from '../../src/core/node.js'
import {WorkerProtocol} from '../../src/core/protocol.js'
import PipelineConfig from '../../src/core/pipeline_config.js'
import Message from '../../src/core/message.js'
import Logger from '../../src/core/logger.js'

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

	test('isStarted', async () => {
		expect.assertions(3)

		const node = new Node(pipelineConfig, protocol)

		let api = {}
		const listener = jest.fn()
		const fn = node => {
			api = node
		}
		await node.set(fn)

		expect(api.isStarted).toBeFalsy()

		await node.start()

		expect(api.isStarted).toBeTruthy()

		await node.stop()

		expect(api.isStarted).toBeFalsy()
	})

	test('isUp', async () => {
		expect.assertions(3)

		const node = new Node(pipelineConfig, protocol)

		let api = {}
		const listener = jest.fn()
		const fn = node => {
			api = node
		}
		await node.set(fn)

		expect(api.isUp).toBeFalsy()

		await node.start()
		await node.up()

		expect(api.isUp).toBeTruthy()

		await node.down()

		expect(api.isUp).toBeFalsy()
	})

	test('isPaused', async () => {
		expect.assertions(3)

		const node = new Node(pipelineConfig, protocol)

		let api = {}
		const listener = jest.fn()
		const fn = node => {
			api = node
		}
		await node.set(fn)

		await node.start()
		await node.up()

		expect(api.isPaused).toBeFalsy()

		await node.pause()

		expect(api.isPaused).toBeTruthy()

		await node.resume()

		expect(api.isPaused).toBeFalsy()
	})

	test('emitter', async () => {
		expect.assertions(3)

		const node = new Node(pipelineConfig, protocol)

		let api = {}
		const listener = jest.fn()
		const fn = node => {
			api = node
		}
		await node.set(fn)

		const handler = jest.fn()
		api.on('test', handler)

		expect(node.emitter.listenerCount('test')).toEqual(1)

		api.off('test', handler)

		expect(node.emitter.listenerCount('test')).toEqual(0)

		api.once('test', handler)

		await node.emitter.emit('test')

		expect(handler).toHaveBeenCalledTimes(1)
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

	test('start: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.onStart(listener)
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

	test('start: up method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.onUp(listener)
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

	test('stop: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.onStop(listener)
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

	test('stop: down method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.onDown(listener)
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

	test('up: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node.onStart(() => node.up())
			node.onUp(listener)
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
			node
				.on(Node.Event.UP, async () => node.down())
				.on(Node.Event.DOWN, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('down: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => {
					await node.down()
				})
				.onDown(listener)
		}
		await node.set(fn)

		await node.start()

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

	test('pause: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.pause())
				.onPause(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('resume: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.pause())
				.onPause(async () => node.resume())
				.on(Node.Event.RESUME, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('resume: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.pause())
				.onPause(async () => node.resume())
				.onResume(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalled()
		expect(listener).toHaveBeenCalledTimes(1)
	})

	test('in: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.in(message))
				.on(Node.Event.IN, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('in: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.in(message))
				.onIn(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('out: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.out(message))
				.on(Node.Event.OUT, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('out: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.out(message))
				.onOut(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('ack: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.ack(message))
				.on(Node.Event.ACK, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('ack: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.ack(message))
				.onAck(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('nack: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.nack(message))
				.on(Node.Event.NACK, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('nack: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.nack(message))
				.onNack(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('ignore: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.ignore(message))
				.on(Node.Event.IGNORE, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('ignore: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.ignore(message))
				.onIgnore(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('reject: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => node.reject(message))
				.on(Node.Event.REJECT, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('reject: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => await node.reject(message))
				.onReject(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(message)
	})

	test('error: listener', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()
		const error = new Error()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => await node.error(error, message))
				.on(Node.Event.ERROR, listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(error, message)
	})

	test('error: method', async () => {
		expect.assertions(2)

		const node = new Node(pipelineConfig, protocol)

		const message = new Message()
		const error = new Error()

		const listener = jest.fn()
		const fn = node => {
			node
				.onUp(async () => await node.error(error, message))
				.onError(listener)
		}
		await node.set(fn)

		await node.start()

		expect(listener).toHaveBeenCalledTimes(1)
		expect(listener).toHaveBeenCalledWith(error, message)
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

	test('log', async () => {
		expect.assertions(8)

		const node = new Node(pipelineConfig, protocol)

		const debug = jest.spyOn(node.log, 'debug')
		const info = jest.spyOn(node.log, 'info')
		const warn = jest.spyOn(node.log, 'warn')
		const error = jest.spyOn(node.log, 'error')

		const fn = node => {
			node.log.debug('debug')
			node.log.info('info')
			node.log.warn('warn')
			node.log.error('error')
		}

		await node.set(fn)

		expect(debug).toHaveBeenCalledTimes(1)
		expect(debug).toHaveBeenCalledWith('debug')

		expect(info).toHaveBeenCalledTimes(1)
		expect(info).toHaveBeenCalledWith('info')

		expect(warn).toHaveBeenCalledTimes(1)
		expect(warn).toHaveBeenCalledWith('warn')

		expect(error).toHaveBeenCalledTimes(1)
		expect(error).toHaveBeenCalledWith('error')
	})
})