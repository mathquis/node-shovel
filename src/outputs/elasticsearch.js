const File          = require('fs')
const Path          = require('path')
const {Client}      = require('@elastic/elasticsearch')
const OutputNode    = require('../output')

const META_INDEX_TEMPLATE = 'elasticsearch_index'

class ElasticsearchOutput extends OutputNode {
  constructor(name, codec, options) {
    super(name, codec, options)

    let ca
    if ( this.getConfig('ca') ) {
      const caPath = Path.resolve(process.cwd(), this.getConfig('ca'))
      ca = File.readFileSync(caPath)
    }

    const scheme = ca ? 'https' : this.getConfig('scheme')

    const opts = {
      node: `${scheme}://${this.getConfig('host')}:${this.getConfig('port')}`,
      auth: {
        username: this.getConfig('username'),
        password: this.getConfig('password')
      },
      ssl: {
        ca,
        rejectUnauthorized: this.getConfig('reject_unauthorized')
      }
    }

    this.client         = new Client(opts)
    this.queue          = []
    this.flushTimeout   = null
  }

  get configSchema() {
    return {
      scheme: {
        doc: '',
        default: 'https',
        arg: 'es-scheme',
        env: 'ELASTICSEARCH_SCHEME'
      },
      host: {
        doc: '',
        default: 'localhost',
        arg: 'es-host',
        env: 'ELASTICSEARCH_HOST'
      },
      port: {
        doc: '',
        format: 'port',
        default: 9200,
        arg: 'es-port',
        env: 'ELASTICSEARCH_PORT'
      },
      username: {
        doc: '',
        default: '',
        env: 'ELASTICSEARCH_USERNAME'
      },
      password: {
        doc: '',
        default: '',
        sensitive: true,
        env: 'ELASTICSEARCH_PASSWORD'
      },
      ca: {
        doc: '',
        default: '',
        arg: 'es-ca-cert',
        env: 'ELASTICSEARCH_CA_CERT'
      },
      reject_unauthorized: {
        doc: '',
        default: true,
        format: Boolean,
        arg: 'es-reject-unauthorized',
        env: 'ELASTICSEARCH_SSL_VERIFY'
      },
      index_name: {
        doc: '',
        default: 'message',
        arg: 'es-index-name',
        env: 'ELASTICSEARCH_INDEX_NAME'
      },
      index_shard: {
        doc: '',
        default: '',
        arg: 'es-index-shard',
        env: 'ELASTICSEARCH_INDEX_SHARD'
      },
      queue_size: {
        doc: '',
        default: 1000,
        format: Number,
        arg: 'es-queue-size'
      },
      queue_timeout: {
        doc: '',
        default: 10000,
        format: Number,
        arg: 'es-queue-timeout'
      },
      fail_timeout: {
        doc: '',
        default: 5000,
        format: Number,
        arg: 'es-fail-timeout'
      },
      template: {
        doc: '',
        default: '',
        arg: 'es-template'
      }
    }
  }

  get indexShardName() {
    const indexShard = this.getConfig('index_shard')
    return this.getConfig('index_name') + ( indexShard ? `-${indexShard}` : '' )
  }

  async setupTemplate() {
    const templateFile = this.getConfig('template')
    if ( templateFile ) {
      const templatePath = Path.resolve(process.cwd(), templateFile)
      this.log.debug('Setting up template "%s"...', templatePath)

      let tpl
      try {
        tpl = require(templatePath)
        if ( typeof tpl === 'function' ) {
          tpl = tpl(this.config)
        }
      } catch (err) {
        this.error(new Error(`Template "${templatePath}" not found: ${err.message}`))
        return
      }

      try {
        await this.client.indices.getTemplate({
          name: tpl.name
        })
        this.log.debug('Template already created')
        return
      } catch (err) {
        this.log.warn('Template "%s" not created', tpl.name)
      }

      try {
        this.log.debug('Creating template...')
        await this.client.indices.putTemplate({
          name: tpl.name,
          body: tpl.template
        })
        this.log.info('Created template')
      } catch (err) {
        this.error(new Error(`Unable to create template: ${err.message}`))
      }
    }
  }

  async start() {
    this.log.debug('Starting...')
    await this.setupTemplate()
    this.log.debug('Connected')
    this.startFlushTimeout()
    await super.start()
  }

  async stop() {
    this.log.debug('Stopping...')
    this.stopFlushTimeout()
    await this.flush()
    await super.stop()
  }

  startFlushTimeout() {
    this.stopFlushTimeout()
    setTimeout(() => {
      this.flush()
    }, this.getConfig('queue_timeout'))
    this.log.debug('Next flush in %dms', this.getConfig('queue_timeout'))
  }

  stopFlushTimeout() {
    if ( !this.flushTimeout ) return
    clearTimeout(this.flushTimeout)
    this.flushTimeout = null
  }

  async write(message) {
    await super.write(message)
    this.queue.push(message)
    if ( this.queue.length >= this.getConfig('queue_size') ) {
      await this.flush()
    }
  }

  async flush() {
    this.stopFlushTimeout()

    if ( this.queue.length > 0 ) {

      const st = (new Date()).getTime()

      this.log.debug('Flushing %d messages...', this.queue.length)

      // Get the queue messages
      const messages = this.queue

      // Empty the queue so new messages can start coming in
      this.queue = []

      // Index the messages
      let response
      try {
        response = await this.client.bulk({
          _source: ['uuid'],
          body: messages.flatMap(message => {
            const indexTemplate = message.getMeta(META_INDEX_TEMPLATE) || this.indexShardName
            return [
              {
                index: {
                  _index: this.renderTemplate(indexTemplate, message).toLowerCase(),
                  _id: message.id
                }
              },
              message.content
            ]
          })
        }, {
          filterPath: 'items.*.error,items.*._id'
        })
      } catch (err) {
        this.error(err)
        // Notify messages processing
        setTimeout(() => {
          messages.forEach(message => {
            this.nack(message)
          })
        }, this.getConfig('fail_timeout'))
        return
      }

      const et = (new Date()).getTime()

      this.log.info('Flushed %d messages in %fms', messages.length, et-st)

      await super.flush()

      // Get messages in error
      const errorIds = new Map()
      if ( response.errors ) {
        response.items.forEach(item => {
          errorIds.set(item._id, true)
        })
      }

      // Notify messages processing
      messages.forEach(message => {
        if ( errorIds.get(message.id) ) {
          this.nack(message)
        } else {
          this.ack(message)
        }
      })

    } else {
      this.log.debug('Nothing to flush')
    }

    this.startFlushTimeout()
  }
}

module.exports = ElasticsearchOutput