const Stream  = require('stream')
const Convict = require('convict')
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

    this.name = name
    this.config = Convict(this.configSchema || {})
    this.isStarted  = false

    const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
      return $1 + '-' + $2.toLowerCase()
    }).toLowerCase()

    this.log = Logger.child({category: type, worker: process.pid})

    this.configure(options)

    this.log.debug('%O', this.config.getProperties())
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
}

module.exports = Node