export default node => {
	node
		.on('in', async (message) => {
			message.decode(message.source)
			node.out(message)
		})
}