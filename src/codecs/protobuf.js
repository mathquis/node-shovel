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
			const className = node.getConfig('class_name') || message.contentType.parameters.get('proto')
			if ( !className ) {
				// TODO: handle no class name
			}
			const messageClass = root.lookupType(className)

			if ( node.getConfig('delimited') ) {
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
				message.content = contents
				node.out(message)
			} else {
				message.content = messageClass.toObject(message.payload)
				node.out(message)
			}
		})
		.on('encode', async (payload) => {
			// TODO
		})


}