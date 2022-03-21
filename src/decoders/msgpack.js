import {unpack} from 'msgpackr'

export default node => {
	node
		.registerConfig({})
		.onIn(async (message) => {
			message.decode(unpack(message.source))
			node.out(message)
		})
}