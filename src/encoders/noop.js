export default node => {
	node
		.onIn(async (message) => {
			message.encode(message.content)
			node.out(message)
		})
}