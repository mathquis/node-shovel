import {pack} from 'msgpackr'
import {testDecoder} from '../utils.js'

const content = {test: 'ok'}

describe('Decoder: Msgpack', () => {
	testDecoder('decode',
		{
			use: 'msgpack'
		},
		pack(content),
		content
	)
})