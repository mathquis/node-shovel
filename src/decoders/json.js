export default node => {
	node
		.registerConfig({
			encoding: {
				doc: '',
				format: ['ascii', 'utf8', 'utf16le'],
				default: 'utf8'
			}
		})
		.on('in', async (message) => {
			const {encoding} = node.getConfig()
			message.decode(JSON.parse(message.source.toString(encoding)))
			node.out(message)
		})
}