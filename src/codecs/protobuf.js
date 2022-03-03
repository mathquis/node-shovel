const Path		= require('path')
const Protobuf	= require('protobufjs')

const META_PROTOBUF_CLASS_NAME = 'protobuf_class_name'

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
			root_path: {
				doc: '',
				format: String,
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
			const rootPath = node.getConfig('root_path')
			if ( !rootPath ) {
				throw new Error('Configuration "root_path" must be defined')
			}
			const sep = rootPath.slice(-1) !== Path.sep ? Path.sep : ''
			const protoPath = node.util.asArray(node.getConfig('proto_path'))
			node.log.debug('Using root "%s"', rootPath)
			root = new Protobuf.Root();
			root.resolvePath = (origin, target) => {
				return rootPath + sep + target;
			}
			await root.load(protoPath)
			protoPath.forEach(file => node.log.info('Loaded proto file "%s" (root: %s)', file, rootPath))
		})
		.on('decode', async message => {
			try {
				if ( node.getConfig('delimited') ) {
					parseDelimitedPayload(message)
					node.out(message)
				} else {
					parsePayload(message)
					node.out(message)
				}
			} catch (err) {
				node.error(err)
				node.reject(message)
			}
		})
		.on('encode', async (message) => {
			try {
				const className = message.getMeta(META_PROTOBUF_CLASS_NAME) || getClassName(message)
				const messageClass = getMessageClass(className)
				if ( node.getConfig('delimited') ) {
					let contents = message.content
					if ( !Array.isArray(content) ) {
						contents = [content]
					}
					message.payload = Buffer.concat(contents.map(content => messageClass.encodeDelimited(content).finish()))
					node.out(message)
				} else {
					message.payload = messageClass.encode(message.content).finish()
				}
				message.setMeta(META_PROTOBUF_CLASS_NAME, className)
				console.log(message)
				node.out(message)
			} catch (err) {
				node.error(err)
				node.reject(message)
			}
		})

	function getClassName(message) {
		const className = node.getConfig('class_name') || message.contentType.parameters.get('proto')
		if ( !className ) {
			throw new Error('Missing class name')
		}
		return className
	}

	function getMessageClass(className) {
		const messageClass = root.lookupType(className)
		if ( !messageClass ) {
			throw new Error(`Unknown class name "${className}"`)
		}
		return messageClass
	}

	function parsePayload(message) {
		const className = getClassName(message)
		const messageClass = getMessageClass(className)
		let msg
		let payload = message.payload
		const contentType = node.getConfig('content_type') || message.contentType.mimeType
		switch ( contentType ) {
			case 'text/json':
			case 'application/json':
				msg = messageClass.fromObject(JSON.parse(payload))
				break
			case 'application/protobuf':
			default:
				msg = messageClass.decode(payload)
				break
		}
		message.setMeta(META_PROTOBUF_CLASS_NAME, className)
		message.content = messageClass.toObject(msg)
	}

	function parseDelimitedPayload(message) {
		const className = getClassName(message)
		const messageClass = getMessageClass(className)
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
		message.setMeta(META_PROTOBUF_CLASS_NAME, className)
		message.content = contents
	}
}