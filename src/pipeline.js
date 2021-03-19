const Node = require('./node')

class Pipeline extends Node {
  constructor(name, fn, options) {
    super(name, options)
    this.fn = fn || ( message => this.push(message) )
  }

  async in(message) {
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
                this.out(message)
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
}

module.exports = Pipeline