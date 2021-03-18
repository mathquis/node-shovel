const Path		= require('path')
const OS		= require('os')
const Convict	= require('convict')
const YAML		= require('js-yaml')
const JSON5		= require('json5')


Convict.addParser([
  { extension: 'json', parse: JSON.parse },
  { extension: 'json5', parse: JSON5.parse },
  { extension: ['yml', 'yaml'], parse: YAML.load }
])

const config = Convict({
	config: {
		doc: '',
		default: '',
		arg: 'config',
		env: 'SERVICE_CONFIG'
	},
	pipeline: {
		doc: '',
		default: '',
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
		route: {
			doc: '',
			default: '/metrics'
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

module.exports = config