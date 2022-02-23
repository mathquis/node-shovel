const configSchema = {}

const codec = (codec, options) => {
	return {
		decode: async (content) => {
			return content
		},

		encode: async (message) => {
			return message.content
		}
	}
}

module.exports = {codec, configSchema}