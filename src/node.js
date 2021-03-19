const Stream  = require('stream')
const Convict = require('convict')
const Pupa    = require('pupa')
const Logger  = require('./logger')

const traverse = (obj, cb) => {
  for (let k in obj) {
  obj[k] = cb(k, obj[k])
    if (obj[k] && typeof obj[k] === 'object') {
      traverse(obj[k], cb)
    }
  }
}

class Node extends Stream.PassThrough {
  constructor(name, options) {
    options || (options = {})
    super({
      objectMode: true
    })

    const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
      return $1 + '-' + $2.toLowerCase()
    }).toLowerCase()

    this.log = Logger.child({category: type, worker: process.pid})

    this.name      = name
    this.isStarted = false
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

  up() {
    this.log.info('Up')
    this.emit('up')
  }

  down() {
    this.log.info('Down')
    this.emit('down')
  }

  error(err) {
    this.emit('error', err)
  }

  getConfig(key) {
    return this.config.get(key)
  }

  ack(message) {
    this.log.debug('Acked (id: %s)', message.id)
    this.emit('ack', message)
  }

  nack(message) {
    this.log.warn('Nacked (id: %s)', message.id)
    this.emit('nack', message)
  }

  reject(message) {
    this.log.warn('Rejected (id: %s)', message.id)
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