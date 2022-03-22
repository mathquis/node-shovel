import File from 'fs'
import Path from 'path'
import YAML from 'js-yaml'
import Convict from 'convict'
import Config from './config.js'
import Logger from './logger.js'
import Utils from './utils.js'

const traverse = (obj, cb) => {
   for (let k in obj) {
      obj[k] = cb(k, obj[k])
      if (obj[k] && typeof obj[k] === 'object') {
         traverse(obj[k], cb)
      }
   }
}

export default class PipelineConfig {
   constructor(pipelineFile) {
      this.config = {}
      this.file = process.cwd()

      const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
         return $1 + '-' + $2.toLowerCase()
      }).toLowerCase()

      this.log = Logger.child({category: type})
   }

   load(pipelineFile) {
      try {
         this.file = Path.resolve(process.cwd(), pipelineFile)
         this.log.debug('Loading pipeline configuration at "%s"', this.file)
         const config = YAML.load(File.readFileSync(this.file))
         this.set(config)
      } catch (err) {
         this.log.error(err.stack)
         throw new Error(`Invalid pipeline "${this.file}" (${err.message})`)
      }
   }

   set(config) {
         traverse(config, (key, value) => {
            if ( value === null ) return value
            if ( typeof value === 'object' && value !== null ) return value
            if ( typeof value === 'boolean' ) return value
            if ( typeof value === 'number' ) return value
            return value
               .replace(/\$\{(.+?)(?::(.+?))?\}/g, (match, env, defaultValue) => {
                  return process.env[env] || defaultValue || ''
               })
         })
         this.config = config
   }

   get path() {
      return Path.dirname(Path.resolve(this.file))
   }

   get name() {
      return this.config.name || '<none>'
   }

   get workers() {
      return this.config.workers || 1
   }

   get input() {
      return this.config.input || {}
   }

   get decoder() {
      return this.config.decoder || {}
   }

   get queue() {
      return this.config.queue || {}
   }

   get pipeline() {
      return this.config.pipeline || {}
   }

   get encoder() {
      return this.config.encoder || {}
   }

   get output() {
      return this.config.output || {}
   }

   toJSON() {
      return this.config
   }
}
