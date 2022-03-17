import Cluster from 'cluster'
import Readline from 'readline'
import Koa from 'koa'
import Router from '@koa/router'
import Prometheus from 'prom-client'
import Config from './config.js'
import Logger from './logger.js'
import {MasterProtocol as Protocol, Event} from './protocol.js'
import AggregatorRegistry from './aggregated_metrics.js'
import Utils from './utils.js'

export default async (pipelineConfigs) => {
   let stopTimeout, aggregatedRegistry

   const log = Logger.child({category: 'master'})

   const protocol = new Protocol()

   log.debug('%O', Config.getProperties())
   log.debug('%O', pipelineConfigs)

   const workers = new Map()
   const roundRobinWorkers = new Map()

   const registry = new AggregatorRegistry()

   const processed = {}

   // Process

   async function exitHandler(event, evtOrExitCodeOrError) {
      try {
         log.debug('Received exit event "%s"', event)
         requestStop()
      } catch (err) {
         log.error(err)
         process.exit(1)
      }
   }

   [
      'beforeExit', 'uncaughtException', 'unhandledRejection',
      'SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP',
      'SIGABRT','SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV',
      'SIGUSR2', 'SIGTERM',
   ].forEach(exitEvent => process.on(exitEvent, exitHandler, exitEvent));

   // Cluster

   let numOnlineWorkers = 0
   const workerGauge = new Prometheus.Gauge({
      name: 'workers',
      help: 'Number of workers',
      labelNames: ['kind']
   })

   workerGauge.set({kind: 'expected'}, workers.size)

   pipelineConfigs.forEach(pipelineConfig => {
      const numWorkers = pipelineConfig.workers
      log.info('Starting workers for pipeline "%s" (workers: %d)', pipelineConfig.name, numWorkers)
      for ( let i = 0 ; i < numWorkers ; i++ ) {
         fork(pipelineConfig)
      }
   })

   Cluster
      .on('online', (worker) => {
         const pipelineConfig = workers.get(worker.id)
         log.debug('Worker %d is online for pipeline "%s"', worker.id, pipelineConfig.name)
         numOnlineWorkers++
         workerGauge.inc({kind: 'online'})
         setTitle()
      })
      .on('disconnect', (worker) => {
         log.debug('Worker "%s" has disconnected', worker.id)
      })
      .on('fork', (worker) => {
         log.debug('Worker "%s" has spawned', worker.id)
      })
      .on('listening', (worker, address) => {
         log.debug('Worker "%s" is listening (address: %s, port: %d, type: %s)', worker.id, address.address, address.port, address.addressType)
      })
      .on('exit', (worker, code, signal) => {
         workerGauge.dec({kind: 'online'})
         numOnlineWorkers--
         const pipelineConfig = workers.get(worker.id)
         if ( code ) {
            log.warn('Worker "%s" died (pipeline: %s, pid: %s, code: %s, signal: %s)', worker.id, pipelineConfig.name, worker.process.pid, code, signal)
         } else {
            log.info('Worker "%s" stopped (pipeline: %s, pid: %s)', worker.id, pipelineConfig.name, worker.process.pid)
         }
         setTitle()
         if ( code === 9 ) {
            log.debug('Starting a new worker...')
            fork(pipelineConfig)
         }
         if ( numOnlineWorkers === 0 ) {
            stop(code)
         }
      })

   // Metrics

   if ( Config.get('metrics.enabled') ) {
      const app = new Koa()
      const router = new Router()

      router.get(Config.get('metrics.route'), async (ctx, next) => {
         try {
            const aggregatedRegistry = await registry.clusterMetrics({
               registries: [Prometheus.register]
            })

            ctx.type = aggregatedRegistry.contentType
            ctx.body = await aggregatedRegistry.metrics()
         } catch (err) {
            log.error(err.message)
            ctx.throw(500)
         }
      })

      app
         .use(router.routes())
         .use(router.allowedMethods())

      app.listen( Config.get('metrics.port') )

      log.info(`Prometheus metrics available on "${Config.get('metrics.route')}" (port: ${Config.get('metrics.port')})`)
   }

   // *****

   function setTitle(title) {
      process.title = title || `Shovel (workers: ${workers.size})`
   }

   async function requestStop(callback) {
      log.debug('Requesting stop from workers')

      for ( let workerId in Cluster.workers ) {
         const worker = Cluster.workers[workerId]
         if ( worker ) {
            protocol.stop(worker)
         }
      }
      stopTimeout = setTimeout(() => {
         stop(1)
      }, Config.get('workers.stop_timeout'))
   }

   async function stop(exitCode = 0) {
      if ( stopTimeout ) {
         clearTimeout(stopTimeout)
         stopTimeout = null
      }

      Object.entries(processed).forEach(([key, metrics]) => {
         log.info('Processed (pipeline: %s, workers: %d, in: %d, acked: %d, nacked: %d, ignored: %d, rejected: %d)', key, metrics.workers, metrics.in, metrics.acked, metrics.nacked, metrics.ignored, metrics.rejected)
      })

      if ( exitCode ) {
         log.warn('Died (exitcode: %d)', exitCode)
      } else {
         log.info('Stopped')
      }

      process.exit(exitCode)
   }

   function findWorkersByPipelineName(pipeline) {
      const list = []
      workers.forEach((config, id) => {
         const worker = Cluster.workers[id]
         if ( config.name === pipeline && worker ) {
            list.push(worker)
         }
      })
      return list
   }

   function broadcast(pipeline, message) {
      findWorkersByPipelineName(pipeline)
         .forEach(worker => {
            protocol.message(worker, message)
         })
   }

   function fanout(pipeline, message) {
      const pipelineWorkers = findWorkersByPipelineName(pipeline)
      const index = roundRobinWorkers.get(pipeline) || 0
      const pos = index % pipelineWorkers.length
      roundRobinWorkers.set(pipeline, pos + 1)
      pipelineWorkers
         .slice(pos, pos + 1)
         .forEach(worker => {
            protocol.message(worker, message)
         })
   }

   function fork(pipelineConfig) {
      const worker = Cluster.fork({
         PIPELINE_PATH: pipelineConfig.file
      })
      workers.set(worker.id, pipelineConfig)
      worker
            .on('message', payload => {
               const {type, workerId} = payload
               const worker = Cluster.workers[workerId]
               log.debug('Received worker event "%s"', type)
               switch ( type ) {
                  case Event.READY:
                     const pipelineConfig = workers.get(worker.id)
                     if ( pipelineConfig ) {
                        log.info('Worker "%s" listening... (pipeline: %s, pid: %s)', worker.id, pipelineConfig.name, worker.process.pid)
                     } else {
                        log.error('Worker "%s" pipeline configuration not found', worker.id)
                     }
                     break
                  case Event.MESSAGE:
                     const {pipelines, mode, message} = payload
                     pipelines.forEach(pipeline => {
                        switch ( mode ) {
                           case 'broadcast':
                              broadcast(pipeline, message)
                              break
                           case 'fanout':
                              fanout(pipeline, message)
                              break
                        }
                     })
                     break
                  case Event.STOPPED:
                     const {pipeline, metrics} = payload.metrics || {}
                     if ( pipeline ) {
                        processed[pipeline] || (processed[pipeline] = {
                           workers: 0,
                           in: 0,
                           acked: 0,
                           nacked: 0,
                           ignored: 0,
                           rejected: 0
                        })

                        processed[pipeline].workers++

                        processed[pipeline] = Object.entries(metrics).reduce((m, [key, value]) => {
                           m[key] += value
                           return m
                        }, processed[pipeline])
                     }
                     break
                  case Event.SHUTDOWN:
                     const {signal} = payload
                     process.emit(signal)
                     break
               }
            })

      log.info('Started worker "%d" (pipeline: %s)', worker.id, pipelineConfig.name)
   }
}