const configSchema = {
	'pretty': {
		doc: '',
		default: true,
		format: Boolean
	}
}

const codec = (codec, options) => {
	return {
		decode: async (content) => {
			return JSON.parse(content.toString('utf8'))
		},

		encode: async (message) => {
			return JSON.stringify(message, null, options.pretty ? 2 : 0)
		}
	}
}

module.exports = {codec, configSchema}