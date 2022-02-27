const Path      = require('path')
const File      = require('fs')
const HTTP      = require('http')
const HTTPS     = require('https')
const Fetch     = require('node-fetch')
const {CronJob} = require('cron')

const META_HTTP_STATUS = 'input_http_status'
const META_HTTP_HEADERS = 'input_http_headers'

module.exports = node => {

   let ca, agent, job

   node
      .registerConfig({
         url: {
            doc: '',
            format: String,
            default: ''
         },
         method: {
            doc: '',
            format: ['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'PATCH'],
            default: 'GET'
         },
         body: {
            doc: '',
            format: String,
            default: ''
         },
         username: {
            doc: '',
            format: String,
            default: ''
         },
         password: {
            doc: '',
            format: String,
            sensitive: true,
            default: ''
         },
         keep_alive: {
            doc: '',
            format: Boolean,
            default: false
         },
         ca_file: {
            doc: '',
            format: String,
            default: ''
         },
         prepare: {
            use: {
               doc: '',
               format: String,
               default: ''
            },
            options: {
               doc: '',
               format: 'options',
               default: {},
               nullable: true
            }
         },
         schedule: {
            doc: '',
            format: String,
            default: '* * * * * *'
         },
         timezone: {
            doc: '',
            format: String,
            default: 'UTC'
         }
      })
      .on('start', async () => {
         const {ca_file, url, keep_alive, schedule, timezone} = node.getConfig()

         if ( ca_file ) {
            ca = loadIfExists(ca_file)
         }

         if ( url.match(/^https:/i) ) {
            agent = new HTTPS.Agent({
               keepAlive: keep_alive,
               cert: ca
            })
         } else {
            agent = new HTTP.Agent({
               keepAlive: keep_alive
            })
         }

         const job = new CronJob(schedule, () => {
            request()
         }, null, true, timezone)

         job.start()
      })
      .on('stop', async () => {
         if ( job ) {
            job.stop()
         }
      })

   function loadIfExists(file) {
      if ( !file ) {
         return
      }
      return File.readFileSync(Path.resolve(node.pipelineConfig.path, file))
   }

   async function request() {
      let {url, method, username, password, body} = node.getConfig()

      method = method.toUpperCase()
      const headers = {}

      // Authentification
      if ( username ) {
         const authorization = `${username}:${password}`
         headers['Authorization'] = `Basic ${authorization.toString('base64')}`
      }

      const req = {
         url,
         method,
         body,
         headers,
         agent
      }

      node.log.info('Requesting endpoint [%s] %s', req.method, req.url)
      node.log.debug('%O', req)

      try {
         node.in()
         const response = await Fetch(req.url, req)
         const messages = await node.decode(response)
         messages.forEach(message => {
            message.setContentType(headers.contentType)
            message.setMetas([
               [META_HTTP_STATUS, response.status],
               [META_HTTP_HEADERS, response.headers],
            ])
            node.out(message)
         })
      } catch (err) {
         node.error(err)
         node.reject()
      }
   }
}