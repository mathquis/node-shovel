import Clone from 'clone'
import Utils from './utils.js'

const defaultContentType = Utils.parseContentType('application/octet-stream')

export default class Message {
   constructor(data) {
      this.setup({...data, uuid: undefined})
   }

   get uuid() {
      return this._data.uuid
   }

   get date() {
      return this._data.date
   }

   set date(date) {
      this._data.date = date
   }

   get headers() {
      return this._data.headers
   }

   get source() {
      return this._data.source
   }

   set source(source) {
      return this._data.source = source
   }

   get payload() {
      return this._data.payload
   }

   set payload(payload) {
      return this._data.payload = payload
   }

   get content() {
      return this._data.content
   }

   set content(content) {
      this._data.content = content
   }

   setup(data) {
      data || (data = {})
      this._data = {
         uuid:     data.uuid || Utils.CUID(),
         date:     data.date || new Date(),
         headers:  new Map(Object.entries(data.headers || {})),
         source:   data.source || null,
         payload:  data.payload || null,
         content:  data.content || {},
      }
      if ( !this.hasHeader('content-type') ) {
         this.setHeader('content-type', defaultContentType)
      }
   }

   clone() {
      const clonedData = Clone(this._data, false)
      return new Message(clonedData)
   }

   decode(content) {
      this.content = content || {}
      return this
   }

   encode(payload) {
      this.payload = payload
      return this
   }

   hasHeader(key) {
      return !!this._data.headers.has(key.toLowerCase())
   }

   getHeader(key) {
      return this._data.headers.get(key.toLowerCase())
   }

   setHeader(key, value) {
      this._data.headers.set(key.toLowerCase(), value)
      return this
   }

   setHeaders(headers) {
      Object.entries(headers).forEach(([key, value]) => {
         this.setHeader(key, value)
      })
      return this
   }

   deleteHeader(key) {
      this._data.headers.delete(key.toLowerCase())
      return this
   }

   incHeader(key, step = 1) {
      const value = this.getHeader(key) || 0
      if ( typeof value !== 'number' ) {
         throw new Error('Only numeric value can be incremented')
      }
      this.setHeader(key, value + step)
   }

   decHeader(key, step = 1) {
      const value = this.getHeader(key) || 0
      if ( typeof value !== 'number' ) {
         throw new Error('Only numeric value can be decremented')
      }
      this.setHeader(key, value - step)
   }

   // Header helper
   setContentType(contentType) {
      if ( contentType ) {
         this._data.headers.set('content-type', Utils.parseContentType(contentType))
      }
      return this
   }

   toObject() {
      return {...this._data, headers: Object.fromEntries(this._data.headers)}
   }

   fromObject(data) {
      this.setup(data)
   }

   toString() {
      return `[Message uuid="${this.uuid}"]`
   }

   toJSON() {
      return this.toObject()
   }
}