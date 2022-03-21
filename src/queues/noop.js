export default node => {
	node
		.onIn(async (message) => {
			node.out(message)
		})
}