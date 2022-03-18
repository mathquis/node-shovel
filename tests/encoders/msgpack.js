import {pack} from 'msgpackr'
import {testEncoder} from '../utils.js'

const content = {test: 'ok'}

describe('Encoder: Msgpack', () => {
	testEncoder('encode',
		{
			use: 'msgpack'
		},
		content,
		pack(content)
	)
})