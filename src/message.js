let messageId = 0

class Message {
	constructor(content) {
		this.id  		= ++messageId
		this.date		= new Date()
		this.content	= content
		this.metas		= {}
	}

	setId(id) {
		this.id = id
	}

	setDate(date) {
		this.date = date
	}

	hasMeta(key) {
		return !!this.getMeta(key)
	}

	getMeta(key) {
		return this.metas[key]
	}

	setMeta(key, value) {
		this.metas[key] = value
	}

	deleteMeta(key) {
		delete this.metas[key]
	}
}

module.exports = Message