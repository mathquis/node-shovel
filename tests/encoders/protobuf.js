import {testEncoder} from '../utils.js'
import Protobuf from 'protobufjs'
import Path from 'path'

describe('Encoder: Protobuf', () => {
	const content = {test: 'ok'}
	const rootPath = './tests/assets/'
	const protoPath = 'test.proto'
	const className = 'Test'

	testEncoder('encode',
		{
			use: 'protobuf',
			options: {
				class_name: className,
				proto_path: protoPath,
				root_path: rootPath
			}
		},
		content,
		async () => {
			const root = new Protobuf.Root();
			const sep = rootPath.slice(-1) !== Path.sep ? Path.sep : ''
			root.resolvePath = (origin, target) => {
				return rootPath + sep + target;
			}
			await root.load(protoPath)
			const messageClass = root.lookupType(className)
			return messageClass.encode(content).finish()
		}
	)

	testEncoder('encode: delimited',
		{
			use: 'protobuf',
			options: {
				class_name: className,
				proto_path: protoPath,
				root_path: rootPath,
				delimited: true
			}
		},
		content,
		async () => {
			const root = new Protobuf.Root();
			const sep = rootPath.slice(-1) !== Path.sep ? Path.sep : ''
			root.resolvePath = (origin, target) => {
				return rootPath + sep + target;
			}
			await root.load(protoPath)
			const messageClass = root.lookupType(className)
			return messageClass.encodeDelimited(content).finish()
		}
	)
})