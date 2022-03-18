import {testDecoder} from '../utils.js'

const content = 'ok'

describe('Decoder: Base64', () => {
	testDecoder('decode',
		{
			use: 'base64'
		},
		Buffer.from(content).toString('base64'),
		content
	)
})