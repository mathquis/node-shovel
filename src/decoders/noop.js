export default node => {
	node
		.onIn(async (message) => {
			message.decode(message.source)
			node.out(message)
		})
}