import Logger from '../../src/core/logger.js'

import {jest} from '@jest/globals'

function clearColors(value) {
	return value.replace(/[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g, '')
}

describe('Logger', () => {

	test('debug', () => {
		console._stdout.write = jest.fn(() => {})

		Logger.setLogLevel('debug')

		Logger.debug('debug')

		expect(clearColors(console._stdout.write.mock.calls[0][0]).trim()).toEqual( expect.stringMatching(/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3} DEBUG log: debug$/) )

		console._stdout.write.mockRestore()
	})

	test('info', () => {
		console._stdout.write = jest.fn(() => {})

		Logger.setLogLevel('debug')

		Logger.info('info')

		expect(clearColors(console._stdout.write.mock.calls[0][0]).trim()).toEqual( expect.stringMatching(/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3} INFO log: info$/) )

		console._stdout.write.mockRestore()
	})

	test('warn', () => {
		console._stdout.write = jest.fn(() => {})

		Logger.setLogLevel('debug')

		Logger.warn('warn')

		expect(clearColors(console._stdout.write.mock.calls[0][0]).trim()).toEqual( expect.stringMatching(/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3} WARN log: warn$/) )

		console._stdout.write.mockRestore()
	})

	test('error', () => {
		console._stdout.write = jest.fn(() => {})

		Logger.setLogLevel('debug')

		Logger.error('error')

		expect(clearColors(console._stdout.write.mock.calls[0][0]).trim()).toEqual( expect.stringMatching(/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3} ERROR log: error$/) )

		console._stdout.write.mockRestore()
	})

	test('level: info', () => {
		console._stdout.write = jest.fn(() => {})

		Logger.setLogLevel('info')

		Logger.debug('debug')

		expect(console._stdout.write).toHaveBeenCalledTimes(0)

		Logger.info('info')

		expect(console._stdout.write).toHaveBeenCalledTimes(1)

		console._stdout.write.mockRestore()
	})
})