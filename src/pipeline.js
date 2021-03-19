const Prometheus = require('prom-client')
const Node       = require('./node')

class Pipeline extends Node {
  constructor(name, fn, options) {
    super(name, options)
    this.fn = fn || ( message => this.push(message) )

    this.counter = new Prometheus.Counter({
      name: 'pipeline_message',
      help: 'Number of input messages',
      labelNames: ['pipeline', 'kind']
    })
  }

  error(err) {
    this.counter.inc({pipeline: this.name, kind: 'error'})
    super.error(err)
  }

  up() {
    // Nothing
  }

  down() {
    // Nothing
  }

  async in(message) {
    this.counter.inc({pipeline: this.name, kind: 'in'})
    await super.in(message)
    try {
      await new Promise(async (resolve, reject) => {
        try {
          await this.fn(message, (err, messages) => {
            if ( !messages ) {
              if ( err ) {
                this.error(err)
                this.reject(message)
              } else {
                this.ignore(message)
              }
            } else {
              messages.forEach(message => {
                this.ack(message)
              })
            }
            resolve()
          })
        } catch (err) {
          reject(err)
        }
      })
    } catch (err) {
      this.error(err)
      this.nack(message)
    }
  }

  ack(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'acked'})
    super.ack(message)
  }

  nack(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'nacked'})
    super.nack(message)
  }

  ignore(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'ignored'})
    super.ignore(message)
  }

  reject(message) {
    this.counter.inc({pipeline: this.name, kind: 'out'})
    this.counter.inc({pipeline: this.name, kind: 'rejected'})
    super.reject(message)
  }
}

module.exports = Pipeline