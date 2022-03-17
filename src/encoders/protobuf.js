import Path from 'path'
import Protobuf from 'protobufjs'

const META_PROTOBUF_CLASS_NAME = 'encoder-protobuf-class-name'

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
			node.up()
		})
		.on('in', async (message) => {
			const className = message.getHeader(META_PROTOBUF_CLASS_NAME) || getClassName(message)
			const messageClass = getMessageClass(className)
			if ( node.getConfig('delimited') ) {
				let contents = message.content
				if ( !Array.isArray(content) ) {
					contents = [content]
				}
				message.encode(Buffer.concat(contents.map(content => messageClass.encodeDelimited(content).finish())))
			} else {
				message.encode(messageClass.encode(message.content).finish())
			}
			message.setHeader(META_PROTOBUF_CLASS_NAME, className)
			node.out(message)
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
}