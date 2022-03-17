import Path from 'path'
import File from 'fs'
import HTTP from 'http'
import HTTPS from 'https'
import Fetch from 'node-fetch'
import { CronJob } from 'cron'

const META_HTTP_STATUS = 'input-http-request-status'
const META_HTTP_HEADERS = 'input-http-request-headers'

export default node => {

   let ca, agent, job, requestTimeout
   let beforeFn = req => req

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
            default: null,
            nullable: true
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
         before: {
            doc: '',
            format: String,
            default: ''
         },
         schedule: {
            doc: '',
            format: String,
            default: '* * * * * *'
         },
         interval: {
            doc: '',
            format: 'duration',
            default: null,
            nullable: true
         },
         timezone: {
            doc: '',
            format: String,
            default: 'UTC'
         }
      })
      .on('start', async () => {
         const {ca_file, url, keep_alive, schedule, timezone, before} = node.getConfig()

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

         if ( before ) {
            beforeFn = node.util.loadFn(before)
            if ( typeof beforeFn !== 'function' ) {
               throw new Error('Configuration "before" must export a function')
            }
         }

         node.up()
      })
      .on('stop', async () => {
         if ( job ) {
            job.stop()
         }
      })
      .on('up', async () => {
         startRequestTimeout()
      })
      .on('pause', async () => {
         stopRequestTimeout()
      })
      .on('resume', async () => {
         startRequestTimeout()
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

      const req = beforeFn({
         url,
         method,
         body,
         headers,
         agent
      })

      node.log.info('Requesting endpoint (method: %s, url: %s)', req.method, req.url)

      const response = await Fetch(req.url, req)

      const message = node.createMessage()

      message.source = await response.text()

      message
         .setContentType(response.headers.contentType)
         .setHeaders({
            [META_HTTP_STATUS]: response.status,
            [META_HTTP_HEADERS]: response.headers
         })

      node.in(message)
   }

   function startRequestTimeout() {
      const {interval, schedule, timezone} = node.getConfig()

      stopRequestTimeout()

      if ( interval ) {
         requestTimeout = setTimeout(async () => {
            await request()
            startRequestTimeout()
         }, interval)
      } else {
         if ( job ) {
            return
         }

         job = new CronJob(schedule, () => {
            request()
         }, null, true, timezone)

         job.start()
      }
   }

   function stopRequestTimeout() {
      if ( !requestTimeout ) return
      clearTimeout(requestTimeout)
      requestTimeout = null
   }
}