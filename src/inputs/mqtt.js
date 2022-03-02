const File = require('fs')
const Path = require('path')
const MQTT = require('mqtt')

const META_MQTT_TOPIC = 'input_mqtt_topic'
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
         },
         username: {
            doc: '',
            format: String,
            default: ''
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
         topics: {
            doc: '',
            format: Array,
            default: ['#']
         },
         reconnect_after_ms: {
            doc: '',
            format: Number,
            default: 5000
         }
      })
      .on('start', async () => {
         // Connect to AMQP
         const {url, username, password, ca, cert, key, reconnect_after_ms: reconnectPeriod, topics} = node.getConfig()

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
               connection.subscribe(topics)
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
            .on('message', async (topic, payload, packet) => {
               const {cmd, retain, qos, dup, properties = {}} = packet
               const props = {cmd, retain, qos, dup, properties}
               const options = {
                  contentType: properties.contentType,
                  metas: [
                     [META_MQTT_TOPIC, topic],
                     [META_MQTT_PROPERTIES, props]
                  ]
               }
               node.in(payload, options)
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
}