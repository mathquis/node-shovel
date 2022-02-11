const MQTT      = require('mqtt')
const File      = require('fs')
const Path      = require('path')
const InputNode = require('../input')

const META_MQTT_TOPIC = 'input_mqtt_topic'
const META_MQTT_PROPERTIES = 'input_mqtt_properties'

class MqttInput extends InputNode {
  get configSchema() {
    return {
      url: {
        doc: '',
        default: 'mqtt://localhost:1883',
        arg: 'mqtt-url',
        env: 'MQTT_URL'
      },
      username: {
        doc: '',
        default: '',
        env: 'MQTT_USERNAME'
      },
      password: {
        doc: '',
        default: '',
        sensitive: true,
        env: 'MQTT_PASSWORD'
      },
      ca: {
        doc: '',
        default: '',
        env: 'MQTT_CA'
      },
      cert: {
        doc: '',
        default: '',
        env: 'MQTT_CERT'
      },
      key: {
        doc: '',
        default: '',
        env: 'MQTT_KEY'
      },
      topics: {
        doc: '',
        format: Array,
        default: ['#']
      },
      reconnect_after_ms: {
        doc: '',
        default: 5000,
        arg: 'amqp-reconnect-after-ms'
      }
    }
  }

  get reconnectAfterMs() {
    const reconnectAfterMs = parseInt(this.getConfig('reconnect_after_ms'))
    if ( !reconnectAfterMs || isNaN(reconnectAfterMs) ) {
      return 500
    }
    return reconnectAfterMs
  }

  loadIfExists(file) {
    if ( !file ) {
      return
    }
    return File.readFileSync(Path.resolve(this.pipelineConfig.path, file))
  }

  async start() {
    await super.start()

    // Connect to AMQP
    const {host: hostname, port, username, password} = this

    this.connection = MQTT.connect(this.getConfig('url'), {
      reconnectPeriod: this.reconnectAfterMs,
      username  : this.getConfig('username'),
      password  : this.getConfig('password'),
      ca: this.loadIfExists(this.getConfig('ca')),
      cert: this.loadIfExists(this.getConfig('cert')),
      key: this.loadIfExists(this.getConfig('key'))
    })


    this.connection
      .on('connect', () => {
        this.log.debug('Connected')
        this.connection.subscribe(this.getConfig('topics'))
        this.up()
      })
      .on('offline', () => {
        this.log.debug('Disconnected')
        this.down()
      })
      .on('close', err => {
        this.log.debug('Connection closed')
        if ( err ) {
          this.error(err)
        }
      })
      .on('error', err => {
        this.error(err)
      })
      .on('message', async (topic, data, packet) => {
        this.in('[MQTT]')
        const {cmd, retain, qos, dup} = packet
        const properties = {cmd, retain, qos, dup}
        let message
        try {
          message = await this.decode(data)
          message.setMetas([
            [META_MQTT_TOPIC, topic],
            [META_MQTT_PROPERTIES, properties]
          ])
          this.out(message)
        } catch (err) {
          message = this.createMessage(data)
          message.setMetas([
            [META_MQTT_TOPIC, topic],
            [META_MQTT_PROPERTIES, properties]
          ])
          this.error(err)
          this.reject(message)
        }
      })
  }

  async stop() {
    if ( this.connection ) {
      await new Promise((resolve, reject) => {
        this.connection.end(false, {}, resolve)
      })
    }
    this.down()
    await super.stop()
  }
}

module.exports = MqttInput