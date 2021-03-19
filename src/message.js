let messageId = 0

class Message {
  constructor(content) {
    this._uuid  = ++messageId
    this.id     = ''
    this.date   = new Date()
    this.content  = content
    this.metas    = {}
  }

  setId(id) {
    this.id = ( id || '' ).toString()
  }

  setDate(date) {
    if ( isNaN(date) ) {
      throw new Error('Invalid date')
    }
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

  setMetas(metas) {
    metas.forEach(([key, value]) => {
      this.setMeta(key, value)
    })
  }

  deleteMeta(key) {
    delete this.metas[key]
  }

  toString() {
    return `[message uuid="${this._uuid}" id="${this.id || '<none>'}"]`
  }
}

module.exports = Message