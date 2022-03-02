const Parser = require('nsyslog-parser')

const SYSLOG_PROPERTIES = 'decoder_syslog_properties'

module.exports = node => {
	node
		.registerConfig({})
		.on('decode', async (message) => {
		    const content = Parser(message.payload.toString())
		    const {message: text, ...properties} = content
		    message.content = text
		    message.setContentType('text/plain')
		    message.setMeta(SYSLOG_PROPERTIES, properties)
			node.out(message)
		})
		.on('encode', async (message) => {
			// TODO
			node.out(message)
		})
}