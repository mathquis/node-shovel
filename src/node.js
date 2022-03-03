const Loadable = require('./loadable')

class NodeOperator extends Loadable {
   constructor(pipelineConfig) {
      super(pipelineConfig)

      this.isStarted        = false
      this.isUp             = false

      this.setupMonitoring()
   }

   get defaultLabels() {
      return {pipeline: this.pipelineConfig.name}
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
         labelNames: ['pipeline', 'kind']
      })
   }

   async start() {
      if ( this.isStarted ) return
      this.isStarted = true
      this.log.info('Started')
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
      this.log.info('Stopped')
   }

   async up() {
      if ( this.isUp ) return
      this.log.info('"^ Up"')
      this.isUp = true
      this.status.set({...this.defaultLabels, kind: 'up'}, 1)
      await this.emit('up')
   }

   async down() {
      if ( !this.isUp ) return
      this.log.info('"v Down"')
      this.isUp = false
      this.status.set({...this.defaultLabels, kind: 'up'}, 0)
      await this.emit('down')
   }

   error(err) {
      this.log.error(err)
      this.counter.inc({...this.defaultLabels, kind: 'error'})
      this.emit('error', err)
   }

   in(message) {
      this.log.debug('<- IN %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'in'})
      this.emit('in', message)
   }

   out(message) {
      this.log.debug('-> OUT %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'out'})
      this.emit('out', message)
   }

   ack(message) {
      this.log.debug('-+ ACK %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'acked'})
      this.emit('ack', message)
   }

   nack(message) {
      this.log.debug('-X NACK %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'nacked'})
      this.emit('nack', message)
   }

   ignore(message) {
      this.log.debug('-- IGNORE %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'ignored'})
      this.emit('ignore', message)
   }

   reject(message) {
      this.log.debug('-! REJECT %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'rejected'})
      this.emit('reject', message)
   }
}

module.exports = NodeOperator