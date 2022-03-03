module.exports = node => {
	let stack = []
	node
		.registerConfig({
			// TODO
		})
		.on('decode', message => {
			if ( message.payload.trim().length === 0 ) {
				const stackedMessage = message.clone(stack.join('\n'))
				stack = []
				node.out(stackedMessage)
			} else {
				stack.push(message.payload.trim())
			}
			node.ack(message)
		})
		.on('encode', message => {

		})
}