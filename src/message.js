const Utils = require('./utils')

let messageId = 0
const defaultContentType = Utils.parseContentType('application/octet-stream')
console.log(defaultContentType)

class Message {
   constructor(content) {
      this._uuid       = ++messageId
      this.id          = this._uuid
      this.date        = new Date()
      this.contentType = defaultContentType
      this.content     = content
      this.metas       = {}
   }

   get uuid() {
      return this._uuid
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

   setContentType(contentType) {
      if ( contentType ) {
         this.contentType = Utils.parseContentType(contentType)
      }
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