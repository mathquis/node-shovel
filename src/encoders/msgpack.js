import {pack} from 'msgpackr'

export default node => {
	node
		.registerConfig({})
		.onIn(async (message) => {
			message.encode(pack(message.content))
			node.out(message)
		})
}