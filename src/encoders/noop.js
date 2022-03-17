export default node => {
	node
		.on('in', async (message) => {
			message.encode(message.content)
			node.out(message)
		})
}