import File from 'fs'
import Path from 'path'
import MQTT from 'mqtt'

const META_MQTT_TOPIC = 'input-mqtt-topic'
const META_MQTT_PROPERTIES = 'input-mqtt-properties'

export default node => {
   let connection, listening

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
      .onStart(async () => {
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
               if ( !listening ) {
                  return
               }
               const {cmd, retain, qos, dup, properties = {}} = packet
               const props = {cmd, retain, qos, dup, properties}

               const message = node.createMessage()

               message.source = payload

               message
                  .setContentType(properties.contentType)
                  .setHeaders({
                     [META_MQTT_TOPIC]: topic,
                     [META_MQTT_PROPERTIES]: props
                  })

               node.in(message)
            })
      })
      .onStop(async () => {
         if ( connection ) {
            await new Promise((resolve, reject) => {
               connection.end(false, {}, resolve)
            })
            connection = null
         }
      })
      .onUp(async () => {
         listening = true
      })
      .onPause(async () => {
         listening = false
      })
      .onResume(async () => {
         listening = true
      })
}