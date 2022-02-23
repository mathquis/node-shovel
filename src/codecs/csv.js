const Codec = require('./noop')
const Parser = require('csv-parse').parse
const Serializer = require('csv-stringify').stringify

class CsvCodec extends Codec {

	get configSchema() {
		return {
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
	}

	async encode(message) {
		return new Promise((resolve, reject) => {
			Serializer(msg, {
				delimiter: this.getConfig('delimiter'),
				escape: this.getConfig('escape'),
				quote: this.getConfig('quote'),
				columns: this.getConfig('columns').length > 0 ? this.getConfig('columns') : false,
				encoding: this.getConfig('encoding')
			}, (err, output) => {
				if (err) {
					reject(err)
					return
				}
				resolve(output)
			})
		})
	}
	async decode(msg) {
		return new Promise((resolve, reject) => {
			Parser(msg, {
				delimiter: this.getConfig('delimiter'),
				escape: this.getConfig('escape'),
				quote: this.getConfig('quote'),
				columns: this.getConfig('columns').length > 0 ? this.getConfig('columns') : false,
				encoding: this.getConfig('encoding')
			}, (err, record) => {
				if (err) {
					reject(err)
					return
				}
				resolve(record)
			})
		})
	}
}

module.exports = CsvCodec