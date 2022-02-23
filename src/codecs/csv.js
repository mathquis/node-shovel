const Parser = require('csv-parse').parse
const Serializer = require('csv-stringify').stringify

const configSchema = {
	delimiter: {
		doc: '',
		format: String,
		default: ','
	},
	escape: {
		doc: '',
		format: String,
		default: '"'
	},
	quote: {
		doc: '',
		format: String,
		default: '"'
	},
	columns: {
		doc: '',
		format: Array,
		default: []
	},
	encoding: {
		doc: '',
		format: ['utf8', 'ucs2', 'utf16le', 'latin1', 'ascii', 'base64', 'hex'],
		default: 'utf8'
	}
}

const codec = (codec, options) => {
	return {
		decode: async (msg) => {
			return new Promise((resolve, reject) => {
				Parser(msg, {
					delimiter: options.delimiter,
					escape: options.escape,
					quote: options.quote,
					columns: options.columns.length > 0 ? options.columns : false,
					encoding: options.encoding
				}, (err, record) => {
					if (err) {
						reject(err)
						return
					}
					resolve(record)
				})
			})
		},

		encode: async (message) => {
			return new Promise((resolve, reject) => {
				Serializer(msg, {
					delimiter: options.delimiter,
					escape: options.escape,
					quote: options.quote,
					columns: options.columns.length > 0 ? options.columns : false,
					encoding: options.encoding
				}, (err, output) => {
					if (err) {
						reject(err)
						return
					}
					resolve(output)
				})
			})
		}
	}
}

module.exports = {codec, configSchema}