const MQTT       = require('mqtt')
const File       = require('fs')
const Path       = require('path')

const META_MQTT_TOPIC      = 'input_mqtt_topic'
const META_MQTT_PROPERTIES = 'input_mqtt_properties'

module.exports = node => {
   let connection

  function loadIfExists(file) {
    if ( !file ) {
      return
    }
    return File.readFileSync(Path.resolve(node.pipelineConfig.path, file))
  }

   node
      .registerConfig({
         url: {
            doc: '',
            format: String,
            default: 'mqtt://localhost:1883',
            arg: 'output-mqtt-url',
         },
         username: {
            doc: '',
            format: String,
            default: '',
         },
         password: {
            doc: '',
            format: String,
            default: '',
            sensitive: true,
         },
         ca: {
            doc: '',
            format: String,
            default: ''
         },
         cert: {
            doc: '',
            format: String,
            default: ''
         },
         key: {
            doc: '',
            format: String,
            default: ''
         },
         topic: {
            doc: '',
            format: String,
            default: ''
         },
         reconnect_after_ms: {
            doc: '',
            format: Number,
            default: 5000,
            arg: 'amqp-reconnect-after-ms'
         }
      })
      .on('start', async () => {
         // Connect to AMQP
         const {
            url,
            username,
            password,
            ca,
            cert,
            key,
            reconnect_after_ms: reconnectPeriod
         } = node.getConfig()

         connection = MQTT.connect(url, {
            reconnectPeriod,
            username,
            password,
            ca: loadIfExists(ca),
            cert: loadIfExists(cert),
            key: loadIfExists(key)
         })

         connection
            .on('connect', () => {
               node.log.debug('Connected')
               node.up()
            })
            .on('offline', () => {
               node.log.debug('Disconnected')
               node.down()
            })
            .on('close', err => {
               node.log.debug('Connection closed')
               if ( err ) {
                  node.error(err)
               }
            })
            .on('error', err => {
               node.error(err)
            })
      })
      .on('stop', async () => {
         if ( connection ) {
            await new Promise((resolve, reject) => {
               connection.end(false, {}, resolve)
               connection = null
            })
         }
      })
      .on('in', async (message) => {
         try {
            if ( !connection ) return
            const content = await node.encode(message)
            if ( !content ) return
            await new Promise((resolve, reject) => {
                  const topicTemplate = message.getMeta(META_MQTT_TOPIC) || node.config.get('topic')
                  const topic = node.util.renderTemplate(topicTemplate, message)
                  node.log.debug('Publishing message to topic "%s"', topic)
                  connection.publish(topic, content, message.getMeta(META_MQTT_PROPERTIES), err => {
                     if (err) {
                        reject(err)
                        return
                     }
                     resolve()
                  })
            })
            node.ack(message)
         } catch (err) {
            node.nack(message)
            node.error(err)
         }
      })
}