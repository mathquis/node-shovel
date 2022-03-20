import {testEncoder} from '../utils.js'

const content = 'ok'

describe('Encoder: Noop', () => {
	testEncoder('encode',
		{
			use: 'noop'
		},
		content,
		content
	)
})