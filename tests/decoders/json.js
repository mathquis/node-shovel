import {testDecoder} from '../utils.js'

const content = {test: 'ok'}

describe('Decoder: JSON', () => {
	testDecoder('decode',
		{
			use: 'json'
		},
		JSON.stringify(content),
		content
	)
})