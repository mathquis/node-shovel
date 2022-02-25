module.exports = codec => {
	return {
		decode: async (data) => {
			return data
		},

		encode: async (message) => {
			return message.content
		}
	}
}