import File from 'fs'
import Path from 'path'
import {Client, errors} from '@elastic/elasticsearch'

const META_INDEX_TEMPLATE = 'output-elasticsearch-index-name'
const META_ERROR          = 'output-elasticsearch-error'

export default node => {
   let client, templateCreated, flushTimeout

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
         template: {
            doc: '',
            format: String,
            default: '',
         },
         batch_size: {
            doc: '',
            format: Number,
            default: 1000
         },
         flush_timeout: {
            doc: '',
            format: 'duration',
            default: '5s'
         }
      })
      .on('start', async () => {
         setupClient()
         try {
            await setupTemplate()
         } catch (err) {
            node.log.warn(err.message)
         }
         await node.up()
      })
      .on('down', async () => {
         await node.pause()
      })
      .on('up', async () => {
         await node.resume()
         startFlushTimeout()
      })
      .on('pause', async () => {
         stopFlushTimeout()
      })
      .on('resume', async () => {
         startFlushTimeout()
      })
      .on('in', async (message) => {
         const {
            batch_size: batchSize
         } = node.getConfig()
         queue.push(message)
         if ( queue.length === batchSize ) {
            flush()
         }
      })

   async function flush() {
      stopFlushTimeout()

      const {
         batch_size: batchSize
      } = node.getConfig()

      const batch = queue.splice(0, batchSize)

      if ( batch.length === 0 ) {
         startFlushTimeout()
         return
      }

      try {
         await setupTemplate()
      } catch (err) {
         node.log.warn(err.message)
         batch.forEach(message => {
            node.nack(message)
         })
         startFlushTimeout()
         return
      }

      node.log.debug('Flushing (messages: %d)', batch.length)

      const errorIds = new Map()

      const st = (new Date()).getTime()

      const {index_name: indexName} = node.getConfig()

      try {
         const body = []

         batch.forEach(message => {
            const indexTemplate = message.getHeader(META_INDEX_TEMPLATE) || indexName
            body.push({
               index: {
                  _index: node.util.renderTemplate(indexTemplate, message).toLowerCase(),
                  _id: message.uuid
               }
            })
            body.push(message.content)
         })

         const data = {
            _source: ['uuid'],
            body
         }
         const response = await client.bulk(data, {
            filterPath: 'items.*.error,items.*._id'
         })

         const et = (new Date()).getTime()

         node.log.info('Flushed (messages: %d, time: %fms)', batch.length, et-st)

         node.up()

         // Get messages in error
         if ( response.errors ) {
            response.items.forEach(({index: result}) => {
               if ( result.status >= 400 ) {
                  node.log.warn(result.error.reason)
                  errorIds.set(result._id, result.error.reason)
               }
            })
         }

      } catch (err) {
         node.error(err)

         if ( err instanceof errors.ConnectionError ) {
            node.down()
         } else if ( err instanceof errors.NoLivingConnectionsError ) {
            node.down()
         }

         batch.forEach(message => {
            errorIds.set(message.id, true)
         })
      }

      // Notify messages processing
      batch.forEach(message => {
         const err = errorIds.get(message.uuid)
         if ( err ) {
            message.setHeader(META_ERROR, err)
            node.reject(message)
         } else {
            node.ack(message)
         }
      })

      startFlushTimeout()
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

      node.log.info('Using index "%s"', node.getConfig('index_name'))

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
               tpl = tpl(node)
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
      stopFlushTimeout()
      const {
         flush_timeout: timeout
      } = node.getConfig()
      flushTimeout = setTimeout(() => {
         flush()
      }, timeout)
   }

   function stopFlushTimeout() {
      if ( !flushTimeout ) return
      clearTimeout(flushTimeout)
      flushTimeout = null
   }
}