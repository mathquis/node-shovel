export default node => {
	node
		.registerConfig({
			format: {
				doc: '',
				format: String,
				default: ''
			}
		})
		.onIn(async (message) => {
			const {format} = node.getConfig()
			message.encode(node.util.renderTemplate(format, message))
			node.out(message)
		})
}