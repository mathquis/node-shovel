import File from 'fs'
import Path from 'path'
import HTTP from 'http'
import HTTPS from 'https'
import Fetch from 'node-fetch'

const META_INDEX_TEMPLATE = 'output-elasticsearch-index-name'
const META_ERROR          = 'output-elasticsearch-error'

const VERSION_LATEST = 'latest'
const VERSION_COMPAT = 'compat'

export default node => {
   let client, agent, templateCreated, flushTimeout, connectivityTimeout, cluster

   let queue = []

   node
      .registerConfig({
         version: {
            doc: '',
            format: ['5', '6', '7', '8', VERSION_LATEST, VERSION_COMPAT],
            default: VERSION_LATEST
         },
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
            default: false
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
         document_type: {
            doc: '',
            format: String,
            nullable: true,
            default: null
         },
         sniffing: {
            doc: '',
            format: Boolean,
            default: false
         },
         warnings: {
            doc: '',
            format: Boolean,
            default: true
         }
      })
      .onStart(async () => {
         const connected = await checkConnectivity()

         const {version} = node.getConfig()
         if ( version === VERSION_COMPAT ) {
            node.log.warn('Using compatibility mode')
         } else {
            node.log.info('Using client (version: %s)', version)
            await setupClient()
         }

         if ( connected ) {
            try {
               await setupTemplate()
            } catch (err) {
               node.log.warn(err.message)
            }
         }

         return connected
      })
      .onUp(async () => {
         stopConnectivityCheck()
      })
      .onDown(async () => {
         startConnectivityCheck()
         queue.forEach(message => node.nack(message))
         queue = []
      })
      .onPause(async () => {
         stopFlushTimeout()
      })
      .onResume(async () => {
         startFlushTimeout()
      })
      .onIn(async (message) => {
         if ( !node.isUp ) {
            node.nack(message)
            return
         }
         const {
            batch_size: batchSize
         } = node.getConfig()
         queue.push(message)
         if ( queue.length === batchSize ) {
            flush()
         }
      })

   async function bulk(batch) {
      const {
         index_name: indexName,
         version
      } = node.getConfig()

      await setupTemplate()

      node.log.debug('Flushing (messages: %d)', batch.length)

      const st = (new Date()).getTime()

      let errorIds
      if ( version === VERSION_COMPAT ) {
         errorIds = await bulkCompat(indexName, batch)
      } else {
         errorIds = await bulkClient(indexName, batch)
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

      const et = (new Date()).getTime()

      node.log.info('Flushed (messages: %d, time: %fms)', batch.length, et-st)

      node.up()
   }

   async function flush() {
      stopFlushTimeout()

      const {
         batch_size: batchSize
      } = node.getConfig()

      const batch = queue.splice(0, batchSize)

      if ( batch.length > 0 ) {
         try {
            await bulk(batch)
         } catch (err) {
            node.error(err)
            node.pause()
            node.down()
            batch.forEach(message => node.nack(message))
         }
      }

      startFlushTimeout()
   }

   async function bulkClient(indexName, batch) {
      const body = []

      const errorIds = new Map()

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

      // Get messages in error
      if ( !!response.body.errors ) {
         response.body.items.forEach(({index: result}) => {
            if ( result.status >= 400 ) {
               node.log.warn(result.error.reason)
               errorIds.set(result._id, result.error.reason)
            }
         })
      }

      return errorIds
   }

   async function bulkCompat(indexName, batch) {
      const {
         warnings,
         document_type: documentType
      } = node.getConfig()

      const errorIds = new Map()

      const body = batch.reduce((body, message) => {
         const indexTemplate = message.getHeader(META_INDEX_TEMPLATE) || indexName
         return body + JSON.stringify({
            index: {
               _index: node.util.renderTemplate(indexTemplate, message).toLowerCase(),
               _type: documentType || undefined,
               _id: message.uuid
            }
         }) + '\n' + JSON.stringify(message.content) + '\n'
      }, '')

      const response = await request('POST', '/_bulk?filter_path=items.*.error,items.*._id', body, {
         'Content-Type': 'application/x-ndjson'
      })

      if ( warnings && response.headers.get('warning') ) {
         node.log.warn(response.headers.get('warning'))
      }

      if ( !response.ok ) {
         throw new Error(response.statusText)
      }

      const responseBody = await response.json()

      // Get messages in error
      if ( responseBody.errors ) {
         responseBody.items.forEach(({index: result}) => {
            if ( result.status >= 400 ) {
               node.log.warn(result.error)
               errorIds.set(result._id, result.error)
            }
         })
      }

      return errorIds
   }

   async function setupClient() {
      templateCreated = false

      let {
         url,
         username,
         password,
         ca_file,
         version,
         sniffing,
         reject_unauthorized: rejectUnauthorized
      } = node.getConfig()

      let ca
      if ( ca_file ) {
         const caPath = Path.resolve(node.pipelineConfig.path, ca_file)
         ca = File.readFileSync(caPath)
      }

      let library
      switch ( version ) {
         case '5':
            library = await import('elasticsearch-5')
            break
         case '6':
            library = await import('elasticsearch-6')
            break
         case '7':
            library = await import('elasticsearch-7')
            break
         case '8':
         case VERSION_LATEST:
            library = await import('elasticsearch-8')
            break
      }

      const {Client} = library

      const opts = {
         node: url,
         auth: {
            username,
            password
         },
         tls: {
            ca,
            rejectUnauthorized
         },
         sniffOnStart: sniffing
      }

      node.log.info('Using index "%s"', node.getConfig('index_name'))

      client = new Client(opts)
   }

   function loadIfExists(file) {
      if ( !file ) {
         return
      }
      return File.readFileSync(Path.resolve(node.pipelineConfig.path, file))
   }

   async function getClusterInfo() {
      cluster = {}

      const response = await request('GET', '/', null, {'Content-Type': 'application/json'})

      cluster = await response.json()

      node.log.info('Connected (cluster: %s, node: %s, version: %s, lucene: %s, min-wire-version: %s)', cluster.cluster_name, cluster.name, cluster.version.number, cluster.version.lucene_version, cluster.version.minimum_wire_compatibility_version)
   }

   async function setupTemplate() {
      if ( templateCreated ) return

      const {template, version} = node.getConfig()
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
            templateCreated = true
            return
         } catch (err) {
            node.log.warn('Template "%s" not created', tpl.name)
         }

         try {
            node.log.debug('Creating template...')
            if ( version === VERSION_COMPAT ) {
               const response = await request('PUT', `/_template/${tpl.name}`, tpl.template, {'Content-Type': 'application/json'})
               if ( response.status >= 400 ) {
                  throw new Error(response.error.reason)
               }
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

   function setupAgent() {
         const {ca_file, url} = node.getConfig()

         let ca
         if ( ca_file ) {
            ca = loadIfExists(ca_file)
         }

         if ( url.match(/^https:/i) ) {
            agent = new HTTPS.Agent({
               cert: ca,
               rejectUnauthorized: false
            })
         } else {
            agent = new HTTP.Agent({})
         }
   }

   async function request(method, path, payload, headers) {
      if ( !agent ) {
         const {
            url,
            ca_file,
            reject_unauthorized: rejectUnauthorized
         } = node.getConfig()

         node.log.info('Using "%s"', url)

         let cert
         if ( ca_file ) {
            node.log.info('Using certificate "%s" (reject: %s)', ca_file, rejectUnauthorized)
            cert = loadIfExists(ca_file)
         }

         if ( url.match(/^https:/i) ) {
            agent = new HTTPS.Agent({
               cert,
               rejectUnauthorized
            })
         } else {
            agent = new HTTP.Agent({})
         }
      }

      let {url, username, password} = node.getConfig()

      method = method.toUpperCase()

      headers || (headers = {})

      // Authentification
      if ( username ) {
         const authorization = `${username}:${password}`
         headers['Authorization'] = `Basic ${Buffer.from(authorization).toString('base64')}`
      }

      const req = {
         url: url + path,
         method,
         body: payload,
         headers,
         agent
      }

      node.log.debug('Requesting compatibility API endpoint (method: %s, url: %s)', req.method, req.url)

      return Fetch(req.url, req)
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

   async function checkConnectivity() {
      try {
         await getClusterInfo()
         await node.up()
         await node.resume()
         return true
      } catch (err) {
         if ( node.isUp ) {
            node.log.warn(err.message)
         }
         node.pause()
         node.down()
         startConnectivityCheck()
         return false
      }
   }

   function startConnectivityCheck() {
      stopConnectivityCheck()
      connectivityTimeout = setTimeout(checkConnectivity, 5000)
   }

   function stopConnectivityCheck() {
      if ( connectivityTimeout ) {
         clearTimeout(connectivityTimeout)
         connectivityTimeout = null
      }
   }
}