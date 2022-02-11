const File   = require('fs')
const Path   = require('path')
const Logger = require('./logger')

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
      ...paths.map(p => Path.resolve(p) + Path.sep + fn),
      modulePath // NPM Module
    ]
    const foundPath = searchPaths.filter(fnPath => !!fnPath).find(fnPath => {
      log.debug('Checking function "%s" in path "%s"...', fn, fnPath)
      return File.existsSync(fnPath) || File.existsSync(fnPath + '.js')
    })
    if ( !foundPath ) {
      throw new Error(`No valid path available for function ${fn}`)
    }
    log.debug('Found function "%s" at path "%s"', fn, foundPath)
    return require(foundPath)
  } catch (err) {
    throw new Error(`Error loading function "${fn}" (${err.message})`)
  }
}

module.exports = {
  loadFn
}