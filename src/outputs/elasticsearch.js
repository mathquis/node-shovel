const File             = require('fs')
const Path             = require('path')
const {Client, errors} = require('@elastic/elasticsearch')
const OutputNode       = require('../output')

const META_INDEX_TEMPLATE = 'elasticsearch_index'

class ElasticsearchOutput extends OutputNode {
  async setup() {
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
      this.log.debug('Setting up template...')

      let tpl
      try {
        tpl = this.pipelineConfig.loadFn(templateFile)
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
    await this.setupTemplate()
    this.startFlushTimeout()
    this.up()
    await super.start()
  }

  async stop() {
    this.stopFlushTimeout()
    await this.flush()
    this.down()
    await super.stop()
  }

  startFlushTimeout() {
    if ( this.flushTimeout ) return
    this.stopFlushTimeout()
    this.flushTimeout = setTimeout(() => {
      this.flush()
    }, this.getConfig('queue_timeout'))
    this.log.debug('Next flush in %dms', this.getConfig('queue_timeout'))
  }

  stopFlushTimeout() {
    if ( !this.flushTimeout ) return
    clearTimeout(this.flushTimeout)
    this.flushTimeout = null
  }

  async in(message) {
    await super.in(message)
    this.queue.push(message)
    if ( this.queue.length >= this.getConfig('queue_size') ) {
      await this.flush()
    }
    this.startFlushTimeout()
  }

  async flush() {
    this.stopFlushTimeout()

    // Get the queue messages
    const messages = this.queue

    // Empty the queue so new messages can start coming in
    this.queue = []

    const errorIds = new Map()

    if ( messages.length > 0 ) {

      const st = (new Date()).getTime()

      this.log.debug('Flushing %d messages...', messages.length)

      // Index the messages
      try {
        const response = await this.client.bulk({
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

        const et = (new Date()).getTime()

        this.log.info('Flushed %d messages in %fms', messages.length, et-st)

        this.up()

        // Get messages in error
        if ( response.errors ) {
          response.items.forEach(item => {
            errorIds.set(item._id, true)
          })
        }

      } catch (err) {
        this.error(err)

        if ( err instanceof errors.ConnectionError ) {
          this.down()
        } else if ( err instanceof errors.NoLivingConnectionsError ) {
          this.down()
        }

        messages.forEach(message => {
          errorIds.set(message.id, true)
        })
      }
    } else {
      this.log.debug('Nothing to flush')
    }

    // Notify messages processing
    messages.forEach(message => {
      if ( errorIds.get(message.id) ) {
        this.nack(message)
      } else {
        this.ack(message)
      }
    })
  }
}

module.exports = ElasticsearchOutput