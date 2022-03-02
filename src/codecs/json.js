module.exports = node => {
	node
		.registerConfig({
			pretty: {
				doc: '',
				default: true,
				format: Boolean
			}
		})
		.on('decode', message => {
			message.content = JSON.parse(message.payload.toString('utf8'))
			node.out(message)
		})
		.on('encode', message => {
			message.payload = JSON.stringify(message.content, null, node.getConfig('pretty') ? 2 : 0)
			node.out(message)
		})
}