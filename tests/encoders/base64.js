import {testEncoder} from '../utils.js'
import { colorOptions } from '../../src/encoders/json.js'
import Colorize from 'json-colorizer'

const content = 'ok'

describe('Encoder: Base64', () => {
	testEncoder('encode',
		{
			use: 'base64'
		},
		content,
		Buffer.from(content).toString('base64')
	)
})