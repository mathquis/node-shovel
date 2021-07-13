module.exports = () => {
	return {
		decode: async msg => {
			return JSON.parse(msg.toString('utf8'))
		}
	}
}