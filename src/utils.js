const File              = require('fs')
const Path              = require('path')
const Pupa              = require('pupa')
const ContentTypeParser = require('whatwg-mimetype')
const Glob              = require('glob')
const Logger            = require('./logger')

const log = Logger.child({category: 'utils'})

function loadFn(fn, paths = []) {
   let modulePath
   try {
      modulePath = require.resolve(fn)
   } catch (err) {
      log.debug('Function "%s" is not a Node.js module', fn)
   }
   try {
      const searchPaths = [
         ...paths.map(p => Path.resolve(p) + Path.sep + fn)
      ]
      const foundPath = searchPaths.filter(fnPath => !!fnPath).find(fnPath => {
         log.debug('Checking function "%s" (path: %s)', fn, fnPath)
         return File.existsSync(fnPath) || File.existsSync(fnPath + '.js')
      })
      if ( !foundPath ) {
         throw new Error(`No valid path available for function ${fn}`)
      }
      log.debug('Found function "%s" (path: %s)', fn, foundPath)
      return require(foundPath)
   } catch (err) {
      throw new Error(`Error loading function "${fn}": ${err.stack}`)
   }
}


function renderTemplate(tpl, message) {
   const {date} = message
   const data = {
      ...message,
      YYYY: date.getFullYear().toString(),
      YY: date.getYear().toString(),
      MM: (date.getUTCMonth()+1).toString().padStart(2, '0'),
      M: (date.getUTCMonth()+1).toString(),
      DD: date.getUTCDate().toString().padStart(2, '0'),
      D: date.getUTCDate().toString()
   }
   return Pupa(tpl, data)
}

function parseContentType(contentType) {
   const {essence: mimeType, parameters} = new ContentTypeParser(contentType)
   return {mimeType, parameters: new Map(parameters.entries())}
}

function translate(value, dictionary, defaultValue) {
   return dictionary[value] || defaultValue
}

function asArray(value) {
   if ( !Array.isArray(value) ) {
      return [value]
   }
   return value
}

async function glob(pattern) {
   return new Promise((resolve, reject) => {
      glob(pattern, (err, files) => {
         if ( err ) {
            reject(err)
            return
         }
         resolve(files)
      })
   })
}

module.exports = {
   loadFn, renderTemplate, parseContentType, translate, asArray, glob
}