import Path from 'path'
import File from 'fs'
import Koa from 'koa'
import Router from '@koa/router'
import BodyParser from 'koa-bodyparser'

const META_HTTP_CONTEXT = 'input-http-server-context'

export default node => {

   let ca, app, server, listening

   const resolvers = new Map()

   node
      .registerConfig({
         // host: {
         //    doc: '',
         //    format: String,
         //    default: 'localhost'
         // },
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

         const {
            ca_file,
            port,
            route,
            username: user,
            password: pass
         } = node.getConfig()

         if ( ca_file ) {
            ca = loadIfExists(ca_file)
         }

         const app = new Koa()
         const router = new Router()

         router
            .all(route, async (ctx, next) => {
               if ( !listening ) {
                  ctx.throw(403, 'Not listening')
                  return
               }

               const status = await new Promise((resolve, reject) => {
                  node.log.debug('Received HTTP request (method: %s, url: %s)', ctx.method, ctx.url)

                  const message = node.createMessage()

                  message.source = ctx.request.body

                  message
                     .setContentType(ctx.get('content-type'))
                     .setHeaders({
                        [META_HTTP_CONTEXT]: ctx
                     })

                  resolvers.set(message.uuid, resolve)

                  node.in(message)
               })

               ctx.status = status

               await next()
            })

         if ( user ) {
            app
               .use(auth({user, pass}))
         }

         app
            .use(BodyParser())
            .use(router.routes())
            .use(router.allowedMethods())

         server = app.listen(port)

         node.log.info('Listening (interface: %s, port: %d, route: %s)', port, route)

         node.up()
      })
      .on('stop', async () => {
         if ( server ) {
            server.close()
         }
      })
      .on('up', async () => {
         listening = true
      })
      .on('down', async () => {
         listening = false
      })
      .on('pause', async () => {
         listening = false
      })
      .on('resume', async () => {
         listening = true
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
      const resolve = resolvers.get(message.uuid)
      if ( resolve ) {
         resolvers.delete(message.uuid)
         resolve(status)
      } else {
         node.log.warning('Unknown message')
      }
   }
}