Protobuf = require('protobufjs')

module.exports = node => {
	let root
	let remainder = Buffer.alloc(0)

	node
		.registerConfig({
			class_name: {
				doc: '',
				format: String,
				default: ''
			},
			proto_path: {
				doc: '',
				format: Array,
				default: ''
			},
			content_type: {
				doc: '',
				format: String,
				default: ''
			},
			delimited: {
				doc: '',
				format: Boolean,
				default: false
			},
			reset: {
				doc: '',
				format: Boolean,
				default: false
			}
		})
		.on('start', async () => {
			root = await Protobuf.load(node.getConfig('proto_path'))
		})
		.on('decode', async message => {
			try {
				const className = node.getConfig('class_name') || message.contentType.parameters.get('proto')
				if ( !className ) {
					throw new Error('Missing class name')
				}
				const messageClass = root.lookupType(className)
				if ( !messageClass ) {
					throw new Error(`Unknown class name "${className}"`)
				}
				if ( node.getConfig('delimited') ) {
					message.content = parseDelimitedPayload(messageClass, message)
					node.out(message)
				} else {
					message.content = messageClass.toObject(parsePayload(messageClass, message))
					node.out(message)
				}
			} catch (err) {
				node.error(err)
				node.reject(message)
			}
		})
		.on('encode', async (payload) => {
			// TODO
		})


	function parsePayload(messageClass, message) {
		let payload = message.payload
		const contentType = node.getConfig('content_type') || message.contentType.mimeType
		switch ( contentType ) {
			case 'text/json':
			case 'application/json':
				return messageClass.fromObject(JSON.parse(payload))
				break
			case 'application/protobuf':
			default:
				return messageClass.decode(payload)
				break
		}
	}

	function parseDelimitedPayload(messageClass, message) {
		const contents = []
		const buf = Buffer.concat([remainder, message.payload])
		let lastPos = 0
		const reader = Protobuf.Reader.create(buf)
		try {
			while ( reader.pos < reader.len ) {
				lastPos = reader.pos
				const proto = messageClass.decodeDelimited(reader)
				const content = messageClass.toObject(proto)
				contents.push(content)
			}
		} catch (err) {
			if ( node.getConfig('reset') ) {
				remainder = Buffer.alloc(0)
			} else {
				remainder = buf.slice(lastPos)
			}
		}
		return contents
	}
}