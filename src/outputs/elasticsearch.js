import File from 'fs'
import Path from 'path'
import HTTP from 'http'
import HTTPS from 'https'
import {Client, errors} from '@elastic/elasticsearch'
import Fetch from 'node-fetch'

const META_INDEX_TEMPLATE = 'output-elasticsearch-index-name'
const META_ERROR          = 'output-elasticsearch-error'

export default node => {
   let client, agent, templateCreated, flushTimeout

   let queue = []

   node
      .registerConfig({
         url: {
            doc: '',
            format: String,
            default: 'http://localhost:9200'
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
         ca_file: {
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
         },
         compat: {
            doc: '',
            format: Boolean,
            default: false
         },
         document_type: {
            doc: '',
            format: String,
            default: 'logs'
         }
      })
      .onStart(async () => {
         if ( node.getConfig('compat') ) {
            node.log.warn('Using compatibility mode')
            setupAgent()
         } else {
            setupClient()
         }
         try {
            await setupTemplate()
         } catch (err) {
            node.log.warn(err.message)
         }
         await node.up()
      })
      .onDown(async () => {
         await node.pause()
      })
      .onUp(async () => {
         await node.resume()
         startFlushTimeout()
      })
      .onPause(async () => {
         stopFlushTimeout()
      })
      .onResume(async () => {
         startFlushTimeout()
      })
      .onIn(async (message) => {
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

      const {
         index_name: indexName,
         compat,
         document_type: documentType
      } = node.getConfig()

      try {
         let response
         if ( compat ) {
            response = await request('POST', '/_bulk?filterPath=items.*.error,items.*._id', batch.reduce((body, message) => {
               const indexTemplate = message.getHeader(META_INDEX_TEMPLATE) || indexName
               return body + JSON.stringify({
                  index: {
                     _index: node.util.renderTemplate(indexTemplate, message).toLowerCase(),
                     _type: documentType,
                     _id: message.uuid
                  }
               }) + '\n' + JSON.stringify(message.content) + '\n'
            }, ''))

            // Get messages in error
            if ( response.errors ) {
               response.items.forEach(({index: result}) => {
                  if ( result.status >= 400 ) {
                     node.log.warn(result.error)
                     errorIds.set(result._id, result.error)
                  }
               })
            }
         } else {
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

            response = await client.bulk(data, {
               filterPath: 'items.*.error,items.*._id'
            })

            // Get messages in error
            if ( response.errors ) {
               response.items.forEach(({index: result}) => {
                  if ( result.status >= 400 ) {
                     node.log.warn(result.error.reason)
                     errorIds.set(result._id, result.error.reason)
                  }
               })
            }
         }

         const et = (new Date()).getTime()

         node.log.info('Flushed (messages: %d, time: %fms)', batch.length, et-st)

         node.up()

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

      let {
         url,
         username,
         password,
         ca_file,
         reject_unauthorized: rejectUnauthorized
      } = node.getConfig()

      let ca
      if ( ca_file ) {
         const caPath = Path.resolve(node.pipelineConfig.path, ca_file)
         ca = File.readFileSync(caPath)
      }

      const opts = {
         node: url,
         auth: {
            username,
            password
         },
         ssl: {
            ca,
            rejectUnauthorized
         }
      }

      node.log.info('Using index "%s"', node.getConfig('index_name'))

      client = new Client(opts)
   }

   function setupAgent() {
         const {ca_file, url} = node.getConfig()

         let ca
         if ( ca_file ) {
            ca = loadIfExists(ca_file)
         }

         if ( url.match(/^https:/i) ) {
            agent = new HTTPS.Agent({
               cert: ca
            })
         } else {
            agent = new HTTP.Agent({})
         }
   }

   async function setupTemplate() {
      if ( templateCreated ) return

      const {template, compat} = node.getConfig()
      if ( template ) {
         node.log.debug('Setting up template...')

         let tpl
         try {
            tpl = await node.util.loadFn(template, [node.pipelineConfig.path])
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
            if ( compat ) {
               const response = await request('PUT', `/_template/${tpl.name}`, tpl.template)
               // TODO: check response
            } else {
               await client.indices.putTemplate({
                  name: tpl.name,
                  body: tpl.template
               })
            }
            node.log.info('Created template')
         } catch (err) {
            throw new Error(`Unable to create template: ${err.message}`)
         }
      }

      templateCreated = true
   }

   async function request(method, path, payload) {
      let {url, username, password} = node.getConfig()

      method = method.toUpperCase()
      const headers = {}

      // Authentification
      if ( username ) {
         const authorization = `${username}:${password}`
         headers['Authorization'] = `Basic ${authorization.toString('base64')}`
      }

      const req = {
         url: url + path,
         method,
         body: payload,
         headers,
         agent
      }

      node.log.debug('Requesting compatibility API endpoint (method: %s, url: %s)', req.method, req.url)

      const response = await Fetch(req.url, req)

      return await response.json()
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