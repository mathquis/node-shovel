import Path from 'path'
import Protobuf from 'protobufjs'

const META_PROTOBUF_CLASS_NAME = 'decoder-protobuf-class-name'

const decodeOptions = {
	longs: String,
	enums: String,
	bytes: String
}

export default node => {
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
		.onStart(async () => {
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
			node.up()
		})
		.onIn(async (message) => {
			if ( node.getConfig('delimited') ) {
				parseDelimitedPayload(message)
			} else {
				parsePayload(message)
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
		let payload = message.source
		const messageContentType = message.getHeader('content-type')
		const contentType = node.getConfig('content_type') || ( messageContentType && messageContentType.mimeType ) || 'application/protobuf'
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
		message.decode(messageClass.toObject(msg, decodeOptions))
		message.setHeader(META_PROTOBUF_CLASS_NAME, className)
		node.out(message)
	}

	function parseDelimitedPayload(message) {
		const className = getClassName(message)
		const messageClass = getMessageClass(className)
		const contents = []
		remainder = Buffer.concat([remainder, message.source])
		let lastPos = 0
		const reader = Protobuf.Reader.create(remainder)
		try {
			while ( reader.pos < reader.len ) {
				lastPos = reader.pos
				const proto = messageClass.decodeDelimited(reader)
				const content = messageClass.toObject(proto, decodeOptions)
				const newMessage = message.clone()
				newMessage
					.decode(content)
					.setHeader(META_PROTOBUF_CLASS_NAME, className)
				node.out(newMessage)
				node.ack(message)
			}
		} catch (err) {
			if ( err instanceof RangeError ) {
				if ( node.getConfig('reset') ) {
					remainder = Buffer.alloc(0)
				} else {
					remainder = remainder.slice(lastPos)
				}
			} else {
				node.error(err)
			}
		}
	}
}