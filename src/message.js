const CUID = require('cuid')
const Utils = require('./utils')

const defaultContentType = Utils.parseContentType('application/octet-stream')

class Message {
   constructor(payload) {
      this._uuid       = CUID()
      this.id          = this._uuid
      this.date        = new Date()
      this.contentType = defaultContentType
      this.payload     = payload
      this.content     = {}
      this.metas       = {}
   }

   get uuid() {
      return this._uuid
   }

   clone(payload, content) {
      const message = new Message(payload)
      message.content = content || {}
      message.contentType = this.contentType
      message.metas = this.metas
      return message
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
      return `[message uuid="${this._uuid}"]`
   }
}

module.exports = Message