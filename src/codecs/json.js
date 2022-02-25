module.exports = codec => {
	codec
		.registerConfig({
			pretty: {
				doc: '',
				default: true,
				format: Boolean
			}
		})

	return {
		decode: async (data) => {
			return JSON.parse(data.toString('utf8'))
		},

		encode: async (message) => {
			return JSON.stringify(message.content, null, codec.getConfig('pretty') ? 2 : 0)
		}
	}
}