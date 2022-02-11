const MQTT       = require('mqtt')
const File       = require('fs')
const Path       = require('path')
const OutputNode = require('../output')

const META_MQTT_TOPIC = 'input_mqtt_topic'
const META_MQTT_PROPERTIES = 'input_mqtt_properties'

class MqttOutput extends OutputNode {
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
      topic: {
        doc: '',
        format: String,
        default: ''
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
  }

  async stop() {
    if ( this.connection ) {
      await new Promise((resolve, reject) => {
        this.connection.end(false, {}, resolve)
        this.connection = null
      })
    }
    this.down()
    await super.stop()
  }

  async in(message) {
    await super.in(message)
    try {
      if ( this.connection ) {
        await this.publish(message)
        this.ack(message)
        return
      } else {
        this.queue.push(message)
      }
    } catch (err) {
      this.nack(message)
      this.error(err)
    }
  }

  async publish(message) {
    const content = await this.encode(message.content)
    await new Promise((resolve, reject) => {
        const topicTemplate = message.getMeta(META_MQTT_TOPIC) || this.getConfig('topic')
        const topic = this.renderTemplate(topicTemplate, message)
        this.log.debug('Publishing message to topic "%s"', topic)
        this.connection.publish(topic, content, message.getMeta(META_MQTT_PROPERTIES), err => {
          if (err) {
            reject(err)
            return
          }
          resolve()
        })
    })
  }
}

module.exports = MqttOutput