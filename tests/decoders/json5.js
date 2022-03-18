import JSON5 from 'json5'
import {testDecoder} from '../utils.js'

const content = {test: 'ok'}

describe('Decoder: JSON5', () => {
	testDecoder('decode',
		{
			use: 'json5'
		},
		JSON5.stringify(content),
		content
	)
})