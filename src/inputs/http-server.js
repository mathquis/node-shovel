const Path       = require('path')
const File       = require('fs')
const Koa        = require('koa')
const Router     = require('@koa/router')
const BodyParser = require('koa-bodyparser')

const META_HTTP_CONTEXT = 'input_http_context'
const META_HTTP_NEXT = 'input_http_next'

module.exports = node => {

   let ca, app, server

   node
      .registerConfig({
         interface: {
            doc: '',
            format: String,
            default: 'localhost'
         },
         port: {
            doc: '',
            format: 'port',
            default: 80
         },
         route: {
            doc: '',
            format: String,
            default: '(.*)'
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
         ca_file: {
            doc: '',
            format: String,
            default: ''
         }
      })
      .on('start', async () => {
         const {ca_file, interface, port, route} = node.getConfig()

         if ( ca_file ) {
            ca = loadIfExists(ca_file)
         }

         const app = new Koa()
         const router = new Router()

         router.all(route, async (ctx, next) => {
            node.log.debug('Received HTTP request (method: %s, url: %s)', ctx.method, ctx.url)
            const options = {
               contentType: ctx.get('content-type'),
               metas: [
                  [META_HTTP_CONTEXT, ctx],
                  [META_HTTP_NEXT, next]
               ]
            }
            node.in(ctx.request.body, options)
         })

         app
            .use(BodyParser())
            .use(router.routes())
            .use(router.allowedMethods())

         server = app.listen(port)

         node.log.info('Listening (interface: %s, port: %d, route: %s)', interface, port, route)

         node.up()
      })
      .on('stop', async () => {
         if ( server ) {
            server.close()
         }
      })
      .on('ack', async (message) => {
         respond(200, message)
      })
      .on('nack', async (message) => {
         respond(520, message)
      })
      .on('ignore', async (message) => {
         respond(200, message)
      })
      .on('reject', async (message) => {
         respond(501, message)
      })

   function loadIfExists(file) {
      if ( !file ) {
         return
      }
      return File.readFileSync(Path.resolve(node.pipelineConfig.path, file))
   }

   async function respond(status, message) {
      const next = message.getMeta(META_HTTP_NEXT)
      if ( typeof next === 'function' ) {
         const ctx = message.getMeta(META_HTTP_CONTEXT)
         if ( ctx ) {
            ctx.response.status = status
         }
         await next()
         node.log.debug('Response %d: %s', status, message)
      }
   }
}