import JSON5 from 'json5'

export default node => {
	node
		.registerConfig({
			pretty: {
				doc: '',
				default: true,
				format: Boolean
			}
		})
		.on('in', async (message) => {
			message.encode(JSON5.stringify(message.content, null, node.getConfig('pretty') ? 2 : 0))
			node.out(message)
		})
}