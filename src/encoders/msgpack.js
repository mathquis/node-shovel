import {pack} from 'msgpackr'

export default node => {
	node
		.registerConfig({})
		.on('in', async (message) => {
			message.encode(pack(message.content))
			node.out(message)
		})
}