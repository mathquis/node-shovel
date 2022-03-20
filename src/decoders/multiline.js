export default node => {
	let stack = []
	node
		.registerConfig({
			// TODO
		})
		.on('in', async (message) => {
			if ( message.payload.trim().length === 0 ) {
				const stackedMessage = node.createMessage()
				message.clone()

				stack = []
				node.out(stackedMessage)
			} else {
				stack.push(message.source.trim())
			}
		})
}