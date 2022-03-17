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
			message.decode(Buffer.from(message.source, 'base64').toString(encoding))
			node.out(message)
		})
}