const Path       = require('path')
const OS         = require('os')
const Convict    = require('convict')
const YAML       = require('js-yaml')
const JSON5      = require('json5')
const Prometheus = require('prom-client')

Convict.addParser([
   { extension: 'json', parse: JSON.parse },
   { extension: 'json5', parse: JSON5.parse },
   { extension: ['yml', 'yaml'], parse: YAML.load }
])

Convict.addFormat({
   name: 'options',
   validate: function(value, schema) {
      if (value === null || value === undefined) {
         return true;
      }

      if (typeof value === 'object') {
         return true;
      }

      return false;
   },
   coerce: function(value) {
      return value || {}
   }
})

const config = Convict({
   config: {
      doc: '',
      default: '',
      arg: 'config',
      env: 'SERVICE_CONFIG'
   },
   // help: {
   //   doc: '',
   //   default: false,
   //   arg: 'help'
   // },
   pipeline: {
      doc: '',
      format: Array,
      default: [],
      arg: 'pipeline',
      env: 'SERVICE_PIPELINE'
   },
   log: {
      level: {
         doc: '',
         format: ['debug', 'info', 'notice', 'warning', 'error'],
         default: 'info',
         arg: 'log-level',
         env: 'SERVICE_LOG_LEVEL'
      }
   },
   workers: {
      doc: '',
      default: OS.cpus().length,
      arg: 'workers',
      env: 'SERVICE_WORKERS'
   },
   metrics: {
      enabled: {
         doc: '',
         default: true,
         arg: 'metrics-enabled'
      },
      labels: {
         doc: '',
         default: [],
         format: Array,
         arg: 'metrics-label'
      },
      route: {
         doc: '',
         default: '/metrics',
         arg: 'metrics-route'
      },
      port: {
         doc: '',
         default: 3001,
         arg: 'metrics-port',
         env: 'SERVICE_METRICS_PORT'
      }
   }
})

const configFile = config.get('config')
if ( !!configFile ) {
   const configFilePath = Path.resolve(process.cwd(), configFile)
   config.loadFile(configFilePath)
}

const defaultLabels = config.get('metrics.labels').reduce((labels, label) => {
   const [key, value] = label.split('=')
   labels[key] = value
   return labels
}, {})

Prometheus.register.setDefaultLabels({
   ...defaultLabels
})

module.exports = config