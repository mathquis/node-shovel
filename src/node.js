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
      await this.emit('up')
      this.isUp = true
      this.status.set({...this.defaultLabels, kind: 'up'}, 1)
      this.log.info('"^ Up"')
   }

   async down() {
      if ( !this.isUp ) return
      await this.emit('down')
      this.isUp = false
      this.status.set({...this.defaultLabels, kind: 'up'}, 0)
      this.log.info('"v Down"')
   }

   error(err) {
      this.log.error(err)
      this.counter.inc({...this.defaultLabels, kind: 'error'})
      this.emit('error', err)
   }

   async in(message) {
      this.log.debug('<- IN %s', message || '')
      this.counter.inc({...this.defaultLabels, kind: 'in'})
      try {
         await this.emit('in', message)
      } catch (err) {
         this.error(err)
         this.reject(message)
      }
   }

   async out(message) {
      this.log.debug('-> OUT %s', message || '')
      await this.emit('out', message)
      this.counter.inc({...this.defaultLabels, kind: 'out'})
   }

   async ack(message) {
      this.log.debug('-+ ACK %s', message || '')
      await this.emit('ack', message)
      this.counter.inc({...this.defaultLabels, kind: 'acked'})
   }

   async nack(message) {
      this.log.debug('-X NACK %s', message || '')
      await this.emit('nack', message)
      this.counter.inc({...this.defaultLabels, kind: 'nacked'})
   }

   async ignore(message) {
      this.log.debug('-- IGNORE %s', message || '')
      await this.emit('ignore', message)
      this.counter.inc({...this.defaultLabels, kind: 'ignored'})
   }

   async reject(message) {
      this.log.debug('-! REJECT %s', message || '')
      await this.emit('reject', message)
      this.counter.inc({...this.defaultLabels, kind: 'rejected'})
   }
}

module.exports = NodeOperator