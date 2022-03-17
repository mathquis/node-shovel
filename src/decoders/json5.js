import JSON5 from 'json5'

export default node => {
	node
		.registerConfig({})
		.on('in', async (message) => {
			message.decode(JSON5.parse(message.source.toString('utf8')))
			node.out(message)
		})
}