import {testDecoder} from '../utils.js'
import Protobuf from 'protobufjs'
import Path from 'path'

describe('Decoder: Protobuf', () => {
	const content = {test: 'ok'}
	const rootPath = './tests/assets/'
	const protoPath = 'test.proto'
	const className = 'Test'

	testDecoder('decode',
		{
			use: 'protobuf',
			options: {
				class_name: className,
				proto_path: protoPath,
				root_path: rootPath
			}
		},
		async () => {
			const root = new Protobuf.Root();
			const sep = rootPath.slice(-1) !== Path.sep ? Path.sep : ''
			root.resolvePath = (origin, target) => {
				return rootPath + sep + target;
			}
			await root.load(protoPath)
			const messageClass = root.lookupType(className)
			return messageClass.encode(content).finish()
		},
		content
	)

	testDecoder('decode: delimited',
		{
			use: 'protobuf',
			options: {
				class_name: className,
				proto_path: protoPath,
				root_path: rootPath,
				delimited: true
			}
		},
		async () => {
			const root = new Protobuf.Root();
			const sep = rootPath.slice(-1) !== Path.sep ? Path.sep : ''
			root.resolvePath = (origin, target) => {
				return rootPath + sep + target;
			}
			await root.load(protoPath)
			const messageClass = root.lookupType(className)
			return messageClass.encodeDelimited(content).finish()
		},
		content
	)

	testDecoder('decode: json',
		{
			use: 'protobuf',
			options: {
				class_name: className,
				proto_path: protoPath,
				root_path: rootPath,
				content_type: 'application/json'
			}
		},
		JSON.stringify(content),
		content
	)
})