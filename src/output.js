const Node = require('./node')

class OutputNode extends Node {
	async write(message) {
		this.emit('incoming', message)
	}

	async flush() {
		this.emit('flush')
	}
}

module.exports = OutputNode