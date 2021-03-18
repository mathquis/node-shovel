const Cluster    = require('cluster')
const File       = require('fs')
const Path       = require('path')
const Prometheus = require('prom-client')
const YAML       = require('js-yaml')
const Config     = require('./config')
const Logger     = require('./logger')

const log = Logger.child({category: 'shovel'})

// Load pipeline
if ( !Config.get('pipeline') ) {
  log.error('Missing pipeline')
  process.exit(9)
}

let pipelineConfig
try {
  pipelineConfig = YAML.load(File.readFileSync(Path.resolve(process.cwd(), Config.get('pipeline'))))
} catch (err) {
  log.error(`Invalid pipeline "${Config.get('pipeline')}" (${err.message}`)
  process.exit(1)
}

if (Cluster.isMaster) {
  require('./master')(pipelineConfig)
} else {
  require('./worker')(pipelineConfig)
}
