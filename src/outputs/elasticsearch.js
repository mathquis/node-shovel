const File             = require('fs')
const Path             = require('path')
const {Client, errors} = require('@elastic/elasticsearch')

const META_INDEX_TEMPLATE = 'elasticsearch_index'

module.exports = node => {
   let client, flushTimeout, templateCreated

   let queue = []

   node
      .registerConfig({
         scheme: {
            doc: '',
            format: ['http', 'https'],
            default: 'https'
         },
         host: {
            doc: '',
            format: String,
            default: 'localhost',
         },
         port: {
            doc: '',
            format: 'port',
            default: 9200,
         },
         username: {
            doc: '',
            format: String,
            default: '',
         },
         password: {
            doc: '',
            format: String,
            default: '',
            sensitive: true,
         },
         ca: {
            doc: '',
            format: String,
            default: '',
         },
         reject_unauthorized: {
            doc: '',
            format: Boolean,
            default: true
         },
         index_name: {
            doc: '',
            format: String,
            default: 'message',
         },
         queue_size: {
            doc: '',
            format: Number,
            default: 1000
         },
         queue_timeout: {
            doc: '',
            format: Number,
            default: 10000
         },
         template: {
            doc: '',
            format: String,
            default: '',
         }
      })
      .on('start', async () => {
         setupClient()
         try {
            await setupTemplate()
         } catch (err) {
            node.log.warn(err.message)
         }
         startFlushTimeout()
         await node.up()
      })
      .on('stop', async () => {
         stopFlushTimeout()
         await flush()
      })
      .on('in', async (message) => {
         queue.push(message)
         if ( queue.length >= node.getConfig('queue_size') ) {
            await flush()
         }
         startFlushTimeout()
      })

   async function flush() {
      stopFlushTimeout()

      try {
         await setupTemplate()
      } catch (err) {
         node.log.warn(err.message)
         startFlushTimeout()
         return
      }

      // Get the queue messages
      const messages = queue

      // Empty the queue so new messages can start coming in
      queue = []

      const errorIds = new Map()

      if ( messages.length > 0 ) {

         const st = (new Date()).getTime()

         node.log.debug('Flushing %d messages...', messages.length)

         // Index the messages
         try {
            const data = {
               _source: ['uuid'],
               body: messages.flatMap(message => {
                  const indexTemplate = message.getMeta(META_INDEX_TEMPLATE) || node.getConfig('index_name')
                  return [
                     {
                        index: {
                           _index: node.util.renderTemplate(indexTemplate, message).toLowerCase(),
                           _id: message.id
                        }
                     },
                     message.content
                  ]
               })
            }
            const response = await client.bulk(data, {
               filterPath: 'items.*.error,items.*._id'
            })

            const et = (new Date()).getTime()

            node.log.info('Flushed %d messages in %fms', messages.length, et-st)

            node.up()

            // Get messages in error
            if ( response.errors ) {
               response.items.forEach(item => {
                  errorIds.set(item._id, true)
               })
            }

         } catch (err) {
            node.error(err)

            if ( err instanceof errors.ConnectionError ) {
               node.down()
            } else if ( err instanceof errors.NoLivingConnectionsError ) {
               node.down()
            }

            messages.forEach(message => {
               errorIds.set(message.id, true)
            })
         }
      } else {
         node.log.debug('Nothing to flush')
      }

      // Notify messages processing
      messages.forEach(message => {
         if ( errorIds.get(message.id) ) {
            node.nack(message)
         } else {
            node.ack(message)
         }
      })
   }

   function setupClient() {
      templateCreated = false

      let {ca, scheme, host, port, username, password, reject_unauthorized} = node.getConfig()

      let caCert
      if ( ca ) {
         const caPath = Path.resolve(node.pipelineConfig.path, ca)
         caCert = File.readFileSync(caPath)
         if ( caCert ) {
            scheme = https
         }
      }

      const opts = {
         node: `${scheme}://${host}:${port}`,
         auth: {
            username,
            password
         },
         ssl: {
            ca: caCert,
            rejectUnauthorized: reject_unauthorized
         }
      }

      node.log.info('Using index: %s', node.getConfig('index_name'))

      client = new Client(opts)
   }

   async function setupTemplate() {
      if ( templateCreated ) return

      const {template} = node.getConfig()
      if ( template ) {
         node.log.debug('Setting up template...')

         let tpl
         try {
            tpl = node.util.loadFn(template, [node.pipelineConfig.path])
            if ( typeof tpl === 'function' ) {
               tpl = tpl(node.getConfig())
            }
         } catch (err) {
            throw new Error(`Template "${template}" not found: ${err.message}`)
         }

         try {
            await client.indices.getTemplate({
               name: tpl.name
            })
            node.log.debug('Template already created')
            return
         } catch (err) {
            node.log.warn('Template "%s" not created', tpl.name)
         }

         try {
            node.log.debug('Creating template...')
            await client.indices.putTemplate({
               name: tpl.name,
               body: tpl.template
            })
            node.log.info('Created template')
         } catch (err) {
            throw new Error(`Unable to create template: ${err.message}`)
         }
      }

      templateCreated = true
   }

   function startFlushTimeout() {
      if ( flushTimeout ) return
      stopFlushTimeout()
      const queueTimeoutMs = node.getConfig('queue_timeout')
      flushTimeout = setTimeout(() => {
         flush()
      }, queueTimeoutMs)
      node.log.debug('Next flush in %dms', queueTimeoutMs)
   }

   function stopFlushTimeout() {
      if ( !flushTimeout ) return
      clearTimeout(flushTimeout)
      flushTimeout = null
   }
}