import {unpack} from 'msgpackr'

export default node => {
	node
		.registerConfig({})
		.on('in', async (message) => {
			message.decode(unpack(message.source))
			node.out(message)
		})
}