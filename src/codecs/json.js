module.exports = () => {
	return {
		encode: async message => {
			return JSON.stringify(message)
		},
		decode: async msg => {
			return JSON.parse(msg.toString('utf8'))
		}
	}
}