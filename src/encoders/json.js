import Colorize from 'json-colorizer'

export	const colorOptions = {
	colors: {
		STRING_KEY: 'cyan',
		STRING_LITERAL: 'whiteBright',
		NUMBER_LITERAL: 'yellow',
		NULL_LITERAL: 'redBright',
		BOOLEAN_LITERAL: 'greenBright'
	}
}

export default node => {
	node
		.registerConfig({
			pretty: {
				doc: '',
				format: Boolean,
				default: false
			},
			colorize: {
				doc: '',
				format: Boolean,
				default: false
			}
		})
		.onIn(async (message) => {
			let payload = JSON.stringify(message.content, null, node.getConfig('pretty') ? 3 : 0)
			if ( node.getConfig('colorize') ) {
				payload = Colorize(payload, colorOptions)
			}
			message.encode(payload)
			node.out(message)
		})
}