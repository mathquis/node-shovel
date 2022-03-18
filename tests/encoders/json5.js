import JSON5 from 'json5'
import {testEncoder} from '../utils.js'
import { colorOptions } from '../../src/encoders/json5.js'
import Colorize from 'json-colorizer'

const content = {test: 'ok'}

describe('Encoder: JSON5', () => {
	testEncoder('encode',
		{
			use: 'json5'
		},
		content,
		JSON5.stringify(content)
	)

	testEncoder('pretty: true',
		{
			use: 'json5',
			options: {
				pretty: true
			}
		},
		content,
		JSON5.stringify(content, null, 3)
	)

	testEncoder('colorize: true',
		{
			use: 'json5',
			options: {
				colorize: true
			}
		},
		content,
		Colorize(JSON5.stringify(content), colorOptions)
	)
})