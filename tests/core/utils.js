import Utils from '../../src/core/utils.js'
import Message from '../../src/core/message.js'

describe('Utils', () => {

	test('renderTemplate', () => {
		const message = new Message()

		const T = message.date.getTime()
		const YYYY = message.date.getFullYear().toString()
		const YY = message.date.getYear().toString()
		const MM = (message.date.getUTCMonth()+1).toString().padStart(2, '0')
		const M = (message.date.getUTCMonth()+1).toString()
		const DD = message.date.getUTCDate().toString().padStart(2, '0')
		const D = message.date.getUTCDate().toString()
		const HH = message.date.getUTCHours().toString().padStart(2, '0')
		const H = message.date.getUTCHours().toString()
		const mm = message.date.getUTCMinutes().toString().padStart(2, '0')
		const m = message.date.getUTCMinutes().toString()
		const ss = message.date.getUTCSeconds().toString().padStart(2, '0')
		const s = message.date.getUTCSeconds().toString()
		const Z = message.date.getTimezoneOffset().toString()
		const DATE_ISO = message.date.toISOString()
		const DATE_STRING = message.date.toString()

		const template = '{T} // {YYYY} // {YY} // {MM} // {M} // {DD} // {D} // {HH} // {H} // {mm} // {m} // {ss} // {s} // {Z} // {DATE_ISO} // {DATE_STRING} // {uuid}'
		const expected = `${T} // ${YYYY} // ${YY} // ${MM} // ${M} // ${DD} // ${D} // ${HH} // ${H} // ${mm} // ${m} // ${ss} // ${s} // ${Z} // ${DATE_ISO} // ${DATE_STRING} // ${message.uuid}`

		const value = Utils.renderTemplate(template, message)

		expect(value).toEqual(expected)
	})

	test('translate', () => {

		const value = 'test'
		const dict = {
			test: 'ok',
			test2: 'notok'
		}

		expect(Utils.translate('test', dict)).toEqual(dict.test)

		expect(Utils.translate('notfound', dict)).toBeUndefined()

		expect(Utils.translate('notfound', dict, 'default')).toEqual('default')
	})

	test('asArray', () => {

		expect(Utils.asArray('test')).toEqual(['test'])

		expect(Utils.asArray(['test'])).toEqual(['test'])
	})

	test('Duration', () => {

		expect(Utils.Duration.parse('5s')).toEqual(5000)

		expect(Utils.Duration.parse(5000)).toEqual(5000)
	})

	test('CUID', () => {

		expect(Utils.CUID()).toEqual(expect.stringMatching(/^c[a-z0-9]{24}$/))

	})
})