const EventEmitter = require('events')
const Convict      = require('convict')
const Pupa         = require('pupa')
const Logger       = require('./logger')

const traverse = (obj, cb) => {
  for (let k in obj) {
  obj[k] = cb(k, obj[k])
    if (obj[k] && typeof obj[k] === 'object') {
      traverse(obj[k], cb)
    }
  }
}

class Node extends EventEmitter {
  constructor(name, options) {
    options || (options = {})
    super()

    const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
      return $1 + '-' + $2.toLowerCase()
    }).toLowerCase()

    this.log = Logger.child({category: type, worker: process.pid})

    this.name      = name
    this.isStarted = false
    this.isUp      = false
    this.config    = Convict(this.configSchema || {})

    this.configure(options)
  }

  async configure(options) {
    traverse(options, (key, value) => {
      return value
        .replace(/\$\{(.+?)(?::(.+?))?\}/, (match, env, defaultValue) => {
          return process.env[env] || defaultValue || ''
        })
    })
    this.config.load(options)
    this.config.validate({allowed: 'strict'})

    this.log.debug('%O', this.config.getProperties())
  }

  async start() {
    this.isStarted = true
    this.log.info('Started')
  }

  async stop() {
    this.isStarted = false
    this.log.info('Stopped')
  }

  help() {
    return this.config.getSchema()
  }

  getConfig(key) {
    return this.config.get(key)
  }

  up() {
    if ( this.isUp ) return
    this.log.info('Up')
    this.isUp = true
    this.emit('up')
  }

  down() {
    if ( !this.isUp ) return
    this.log.warn('Down')
    this.isUp = false
    this.emit('down')
  }

  error(err) {
    this.log.error(err.message)
    this.emit('error', err)
  }

  async in(message) {
    this.log.debug('<- IN %s', message)
    this.emit('in', message)
  }

  ack(message) {
    this.log.debug('-> ACK %s', message)
    this.emit('ack', message)
  }

  nack(message) {
    this.log.debug('-> NACK %s', message)
    this.emit('nack', message)
  }

  ignore(message) {
    this.log.debug('-> IGNORE %s', message)
    this.emit('ignore', message)
  }

  reject(message) {
    this.log.debug('-> REJECT %s', message)
    this.emit('reject', message)
  }

  renderTemplate(tpl, message) {
    const {date} = message
    const data = {
      ...message,
      YYYY: date.getFullYear().toString(),
      YY: date.getYear().toString(),
      MM: (date.getUTCMonth()+1).toString().padStart(2, '0'),
      M: (date.getUTCMonth()+1).toString(),
      DD: date.getUTCDate().toString().padStart(2, '0'),
      D: date.getUTCDate().toString()
    }
    return Pupa(tpl, data)
  }
}

module.exports = Node