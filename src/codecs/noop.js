module.exports = node => {
	node
		.on('decode', message => {
			message.content = message.payload
			node.out(message)
		})
		.on('encode', message => {
			message.payload = message.content
			node.out(message)
		})
}