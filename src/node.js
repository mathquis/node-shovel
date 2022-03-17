import Loadable from './loadable.js'
import Message from './message.js'

export default class NodeOperator extends Loadable {
   constructor(pipelineConfig, protocol) {
      super(pipelineConfig)

      this._protocol        = protocol
      this.isStarted        = false
      this.isUp             = false
      this.isPaused         = false

      this.setupMonitoring()
   }

   get defaultLabels() {
      return {pipeline: this.pipelineConfig.name}
   }

   get protocol() {
      return this._protocol
   }

   setupMonitoring() {
      this.status = new Prometheus.Gauge({
         name: 'node_status',
         help: 'Status of the node',
         labelNames: ['pipeline', 'kind']
      })

      this.counter = new Prometheus.Counter({
         name: 'node_message',
         help: 'Number of messages',
         labelNames: ['pipeline', 'kind', 'type']
      })
   }

   createMessage(data) {
      return new Message(data)
   }

   shutdown() {
      this.log.info('Shutting down pipeline')
      process.emit('shutdown')
   }

   pipe(node) {
      this
         .on('out', async message => {
            node.in(message)
         })

      node
         .on('ack', message => {
            this.ack(message)
         })
         .on('nack', message => {
            this.nack(message)
         })
         .on('ignore', message => {
            this.ignore(message)
         })
         .on('reject', message => {
            this.reject(message)
         })
         .on('pause', () => {
            this.pause()
         })
         .on('resume', () => {
            this.resume()
         })

      return node
   }

   async start() {
      if ( this.isStarted ) return
      await this.load()
      this.isStarted = true
      this.log.debug('Started')
      const results = await this.emit('start')
      if ( !results ) {
         await this.up()
      }
   }

   async stop() {
      if ( !this.isStarted ) return
      await this.down()
      await this.emit('stop')
      this.isStarted = false
      this.log.debug('Stopped')
   }

   async up() {
      if ( this.isUp ) return
      this.isUp = true
      this.status.set({...this.defaultLabels, kind: 'up'}, 1)
      await this.emit('up')
      this.log.info('"^ Up"')
   }

   async down() {
      if ( !this.isUp ) return
      this.isUp = false
      this.status.set({...this.defaultLabels, kind: 'down'}, 0)
      await this.emit('down')
      this.log.info('"v Down"')
   }

   async pause() {
      if ( !this.isUp ) return
      if ( this.isPaused ) return
      this.isPaused = true
      this.log.info('| Paused')
      await this.emit('pause')
      this.counter.inc({...this.defaultLabels, kind: 'pause'})
   }

   async resume() {
      if ( !this.isUp ) return
      if ( !this.isPaused ) return
      this.isPaused = false
      this.log.info('> Resumed')
      await this.emit('resume')
      this.counter.inc({...this.defaultLabels, kind: 'resume'})
   }

   error(err) {
      let errorMessage = err.stack
      if ( err.origin ) {
         errorMessage += '\n\nMESSAGE:\n' + JSON.stringify(err.origin, null, 3)
      }
      this.log.error(errorMessage)
      this.counter.inc({...this.defaultLabels, kind: 'error'})
      this.emit('error', err)
   }

   async in(message) {
      this.log.debug('<- IN %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'in'})
      try {
         await this.emit('in', message)
      } catch (err) {
         err.origin = message
         this.error(err)
         this.reject(message)
      }
   }

   out(message) {
      this.log.debug('-> OUT %s', message || '')
      this.emit('out', message)
      this.counter.inc({...this.defaultLabels, kind: 'out'})
   }

   ack(message) {
      this.log.debug('-+ ACK %s', message || '')
      this.emit('ack', message)
      this.counter.inc({...this.defaultLabels, kind: 'acked'})
   }

   nack(message) {
      this.log.debug('-X NACK %s', message || '')
      this.emit('nack', message)
      this.counter.inc({...this.defaultLabels, kind: 'nacked'})
   }

   ignore(message) {
      this.log.debug('-- IGNORE %s', message || '')
      this.emit('ignore', message)
      this.counter.inc({...this.defaultLabels, kind: 'ignored'})
   }

   reject(message) {
      this.log.debug('-! REJECT %s', message || '')
      this.emit('reject', message)
      this.counter.inc({...this.defaultLabels, kind: 'rejected'})
   }
}