const Codec = require('./noop')

class JsonCodec extends Codec {
	get configSchema() {
		return {
			'pretty': {
			doc: '',
			default: true,
			format: Boolean,
			arg: 'stdout-pretty'
			}
		}
	}

	async encode(message) {
		return JSON.stringify(message, null, this.getConfig('pretty') ? 2 : 0)
	}
	async decode(msg) {
		return JSON.parse(msg.toString('utf8'))
	}
}

module.exports = JsonCodec