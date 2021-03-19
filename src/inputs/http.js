const Path      = require('path')
const File      = require('fs')
const HTTP      = require('http')
const HTTPS     = require('https')
const Fetch     = require('node-fetch')
const {CronJob} = require('cron')
const Input     = require('../input')

const META_HTTP_STATUS = 'input_http_status'
const META_HTTP_HEADERS = 'input_http_headers'

class HttpInput extends Input {
  get configSchema() {
    return {
      url: {
        doc: '',
        default: ''
      },
      method: {
        doc: '',
        default: 'GET'
      },
      body: {
        doc: '',
        default: ''
      },
      username: {
        doc: '',
        default: ''
      },
      password: {
        doc: '',
        default: ''
      },
      ca_file: {
        doc: '',
        default: ''
      },
      prepare: {
        use: {
          doc: '',
          default: ''
        },
        options: {
          doc: '',
          format: Object,
          default: {}
        }
      },
      schedule: {
        doc: '',
        default: '* * * * * *'
      },
      timezone: {
        doc: '',
        default: 'UTC'
      }
    }
  }

  async setup() {
    this.ca
    if ( this.getConfig('ca_file') ) {
      const caPath = Path.resolve(process.cwd(), this.getConfig('ca'))
      this.ca = File.readFileSync(caPath)
    }

    if ( this.getConfig('url').match(/^https:/i) ) {
      this.agent = new HTTPS.Agent({
        keepAlive: true,
        cert: this.ca
      })
    } else {
      this.agent = new HTTP.Agent({
        keepAlive: true
      })
    }

    const schedule = this.getConfig('schedule')
    if ( schedule ) {
      this.job = new CronJob(schedule, () => {
        this.request()
      }, null, true, this.getConfig('timezone'))
    }

    this.prepareFn = req => req
    const {use, options} = this.getConfig('prepare')
    if ( use ) {
      this.prepareFn = this.pipelineConfig.loadFn(use)(options)
    }
  }

  async start() {
    if ( this.job ) {
      this.job.start()
    }
    await super.start()
  }

  async stop() {
    if ( this.job ) {
      this.job.stop()
    }
    await super.stop()
  }

  async request() {
    const url   = this.getConfig('url')
    const method  = this.getConfig('method').toUpperCase()
    const headers   = {}

    // Authentification
    const username = this.getConfig('username')
    if ( username ) {
      const password = this.getConfig('password')
      const authorization = `${username}:${password}`
      headers['Authorization'] = `Basic ${authorization.toString('base64')}`
    }

    const body = method === 'GET' ? undefined : this.getConfig('body')

    const req = await this.prepareFn({
      url,
      method,
      body,
      headers,
      agent: this.agent
    })

    this.log.info('Requesting endpoint [%s] %s', req.method, req.url)
    this.log.debug('%O', req)

    try {
      const response = await Fetch(req.url, req)
      const message = await this.decode(response)
      message.setMetas([
        [META_HTTP_STATUS, response.status],
        [META_HTTP_HEADERS, response.headers],
      ])
      this.out(message)
    } catch (err) {
        this.error(err)
          this.reject()
    }
  }
}

module.exports = HttpInput