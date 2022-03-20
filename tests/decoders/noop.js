import {testDecoder} from '../utils.js'

const content = 'ok'

describe('Decoder: Noop', () => {
	testDecoder('decode',
		{
			use: 'noop'
		},
		content,
		content
	)
})