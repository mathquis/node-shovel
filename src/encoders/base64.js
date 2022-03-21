export default node => {
	node
		.registerConfig({
			encoding: {
				doc: '',
				format: ['ascii', 'utf8', 'utf16le'],
				default: 'utf8'
			}
		})
		.onIn(async (message) => {
			const {encoding} = node.getConfig()
			message.encode(Buffer.from(message.content, encoding).toString('base64'))
			node.out(message)
		})
}