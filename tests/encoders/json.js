import {testEncoder} from '../utils.js'
import { colorOptions } from '../../src/encoders/json.js'
import Colorize from 'json-colorizer'

const content = {test: 'ok'}

describe('Encoder: JSON', () => {
	testEncoder('encode',
		{
			use: 'json'
		},
		content,
		JSON.stringify(content)
	)

	testEncoder('pretty: true',
		{
			use: 'json',
			options: {
				pretty: true
			}
		},
		content,
		JSON.stringify(content, null, 3)
	)

	testEncoder('colorize: true',
		{
			use: 'json',
			options: {
				colorize: true
			}
		},
		content,
		Colorize(JSON.stringify(content), colorOptions)
	)
})