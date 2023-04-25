import { pathToFileURL } from 'node:url'
import File from 'fs'
import Path from 'path'
import ContentTypeParser from 'whatwg-mimetype'
import {Glob} from 'glob'
import CUID from 'cuid'
import Pupa from 'pupa'
import DurationParser from 'parse-duration'
import {DateTime} from 'luxon'
import Logger from './logger.js'

const log = Logger.child({category: 'utils'})

async function loadFn(fn, paths = []) {
   try {
      moduleImport = await import(fn)
      return moduleImport.default
   } catch (err) {
      log.debug('Function "%s" is not a Node.js module', fn)
   }
   try {
      const searchPaths = [
         ...paths.map(p => Path.resolve(p) + Path.sep + fn)
      ]
      let foundPath = searchPaths
         .filter(fnPath => !!fnPath)
         .find(fnPath => {
            log.debug('Checking function "%s" (path: %s)', fn, fnPath)
            return File.existsSync(fnPath) || File.existsSync(fnPath + '.js')
         })
      if ( !foundPath ) {
         throw new Error(`No valid path available for function ${fn}`)
      }
      if ( !foundPath.match(/\.js$/) ) {
         foundPath += '.js'
      }
      log.debug('Found function "%s" (path: %s)', fn, foundPath)
      const importedFn = await import(pathToFileURL(foundPath))
      return importedFn.default
   } catch (err) {
      throw new Error(`Error loading function "${fn}": ${err.stack}`)
   }
}

function renderTemplate(tpl, message) {
   const {date = new Date()} = message
   const data = {
      ...message.toObject(),
      T: date.getTime(),
      YYYY: date.getFullYear().toString(),
      YY: date.getYear().toString(),
      MM: (date.getUTCMonth()+1).toString().padStart(2, '0'),
      M: (date.getUTCMonth()+1).toString(),
      DD: date.getUTCDate().toString().padStart(2, '0'),
      D: date.getUTCDate().toString(),
      HH: date.getUTCHours().toString().padStart(2, '0'),
      H: date.getUTCHours().toString(),
      mm: date.getUTCMinutes().toString().padStart(2, '0'),
      m: date.getUTCMinutes().toString(),
      ss: date.getUTCSeconds().toString().padStart(2, '0'),
      s: date.getUTCSeconds().toString(),
      Z: date.getTimezoneOffset().toString(),
      DATE_ISO: date.toISOString(),
      DATE_STRING: date.toString()
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
      Glob(pattern, (err, files) => {
         if ( err ) {
            reject(err)
            return
         }
         resolve(files)
      })
   })
}

const Duration = {
   parse: function(value) {
      return DurationParser(value)
   }
}

const Utils = {
   loadFn, renderTemplate, parseContentType, translate, asArray, glob, Duration, CUID, Time: DateTime
}

export default Utils