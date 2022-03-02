const File     = require('fs')
const Path     = require('path')
const YAML     = require('js-yaml')
const Logger   = require('./logger')
const Utils    = require('./utils')

const traverse = (obj, cb) => {
   for (let k in obj) {
      obj[k] = cb(k, obj[k])
      if (obj[k] && typeof obj[k] === 'object') {
         traverse(obj[k], cb)
      }
   }
}

class PipelineConfig {
   constructor(pipelineFile) {
      this.file = Path.resolve(process.cwd(), pipelineFile)

    const type = this.constructor.name.replace(/(.)([A-Z])/g, (_, $1, $2) => {
      return $1 + '-' + $2.toLowerCase()
    }).toLowerCase()

    this.log = Logger.child({category: type})
   }

   load() {
      try {
         this.log.info('Loading pipeline configuration at "%s"', this.file)
         const config = YAML.load(File.readFileSync(this.file))
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
      } catch (err) {
         this.log.error(err.stack)
         throw new Error(`Invalid pipeline "${this.file}" (${err.message}`)
      }
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

module.exports = PipelineConfig