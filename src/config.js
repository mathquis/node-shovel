import Path from 'path'
import OS from 'os'
import Convict from 'convict'
import YAML from 'js-yaml'
import JSON5 from 'json5'
import Prometheus from 'prom-client'
import Utils from './utils.js'

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

Convict.addFormat({
   name: 'duration',
   validate: function(value, schema) {
      if ( null === value ) {
         if ( schema.nullable ) {
            return true
         } else {
            throw new Error('must not be null')
         }
      }
      const type = typeof value
      return ( type === 'string' || type === 'number' )
   },
   coerce: function(value) {
      if ( null === value ) {
         return null
      }
      return Utils.Duration.parse(value)
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
      stop_timeout: {
         doc: '',
         format: 'duration',
         default: '60s'
      }
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
      },
      refresh: {
         doc: '',
         format: 'duration',
         default: '10s'
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

export default config