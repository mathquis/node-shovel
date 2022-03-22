import Message from '../../src/core/message.js'

import {jest} from '@jest/globals'

describe('Message', () => {

	const uuid = 'test'
	const date = new Date('2010-01-02T03:04:05.000Z')
	const headers = new Map(Object.entries({

	}))
	const source = 'source'
	const content = {test: 'ok'}
	const payload = 'payload'

	test('instance', async () => {
		const message = createMessage()

		expect(message.uuid).not.toEqual(uuid)
		expect(message.date).toBe(date)
		expect(message.source).toBe(source)
		expect(message.content).toBe(content)
		expect(message.payload).toBe(payload)

		expect(message.headers.get('content-type')).toEqual({'mimeType': 'application/octet-stream', parameters: new Map()})

		const newDate = new Date()
		message.date = newDate

		expect(message.date).toBe(newDate)

		message.setHeader('test', 'ok')

		expect(message.getHeader('test')).toEqual('ok')

		message.setHeaders({
			param1: 'ok1',
			param2: 'ok2'
		})

		expect(message.getHeader('param1')).toEqual('ok1')
		expect(message.getHeader('param2')).toEqual('ok2')

		message.deleteHeader('test')

		expect(message.getHeader('test')).toBeUndefined()

		expect(() => {
			message.incHeader('param1')
		}).toThrow()

		expect(() => {
			message.decHeader('param1')
		}).toThrow()

		message.setHeader('count', 1)
		message.incHeader('count')

		expect(message.getHeader('count')).toEqual(2)

		message.incHeader('count', 2)

		expect(message.getHeader('count')).toEqual(4)

		message.decHeader('count')

		expect(message.getHeader('count')).toEqual(3)

		message.decHeader('count', 2)

		expect(message.getHeader('count')).toEqual(1)

		message.setContentType('application/json; disposition=inline')

		expect(message.getHeader('content-type').mimeType).toEqual('application/json')
		expect(message.getHeader('content-type').parameters.get('disposition')).toEqual('inline')

		const decoded = {decoded: 'ok'}
		message.decode(decoded)

		expect(message.content).toBe(decoded)

		message.decode()

		expect(message.content).toEqual({})

		message.incHeader('noinc', 2)

		expect(message.getHeader('noinc')).toEqual(2)

		message.decHeader('nodec', 2)

		expect(message.getHeader('nodec')).toEqual(-2)

		message.setContentType()

		expect(message.getHeader('content-type').mimeType).toEqual('application/json')
		expect(message.getHeader('content-type').parameters.get('disposition')).toEqual('inline')

		expect(message.toJSON()).toEqual(message.toObject())
	})

	test('empty', async () => {
		const message = new Message()

		message.setup()

		expect(message.uuid).not.toEqual('')
		expect(message.date).toBeInstanceOf(Date)
		expect(message.content).toEqual({})
		expect(message.source).toEqual(null)
		expect(message.payload).toEqual(null)
	})

	test('clone', async () => {
		const message = createMessage()

		const clone = message.clone()

		expect(clone.uuid).not.toEqual(message.uuid)
		expect(clone.date).not.toBe(message.date)
		expect(clone.headers).not.toBe(message.headers)
		expect(clone.content).not.toBe(message.content)
		expect(clone.date).toEqual(date)
		expect(clone.source).toEqual(source)
		expect(clone.content).toEqual(content)
		expect(clone.payload).toEqual(payload)

		expect(clone.getHeader('content-type')).toEqual({'mimeType': 'application/octet-stream', parameters: new Map()})
	})

	test('fromObject', async () => {
		const message = createMessage()

		const clone = new Message()
		clone.fromObject(message.toObject())

		expect(clone.uuid).toEqual(message.uuid)
		expect(clone.date).toEqual(message.date)
		expect(clone.headers).toEqual(message.headers)
		expect(clone.content).toEqual(message.content)
		expect(clone.payload).toEqual(message.payload)
	})

	function createMessage() {
		const data = {
			uuid,
			date,
			headers,
			source,
			payload,
			content,
		}

		return new Message(data)
	}
})