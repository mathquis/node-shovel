import File from 'fs'
import Path from 'path'
import Readline from 'readline'
import Cluster from 'cluster'
import Prometheus from 'prom-client'
import Logger from './logger.js'
import Processor from './processor.js'
import {WorkerProtocol as Protocol, Event} from './protocol.js'
import AggregatorRegistry from './aggregated_metrics.js'
import Utils from './utils.js'

export default async (pipelineConfig) => {
   let pipeline, protocol

   async function exitHandler(code) {
      // Nothing
   }

   [
      'beforeExit',
      'uncaughtException',
      'unhandledRejection',
      'SIGHUP',
      'SIGINT',
      'SIGQUIT',
      'SIGILL',
      'SIGTRAP',
      'SIGABRT','SIGBUS',
      'SIGFPE',
      'SIGUSR1',
      'SIGSEGV',
      'SIGUSR2',
      'SIGTERM'
   ].forEach(exitEvent => process.on(exitEvent, exitHandler))

   const log = Logger.child({category: 'worker', pipeline: pipelineConfig.name, worker: Cluster.worker.id})

   try {
      Prometheus.collectDefaultMetrics()

      log.debug('Starting (pipeline: %s)', pipelineConfig.name)

      protocol = new Protocol()

      pipeline = new Processor(pipelineConfig, protocol)

      process
         .on('message', async ({type, message}) => {
            switch ( type ) {
               case Event.STOP:
                  shutdown()
                  break

               case Event.MESSAGE:
                  pipeline.in(message)
                  break
            }
         })
         .on('shutdown', async () => {
            shutdown()
         })
         .on('uncaughtException', async err => {
            log.error(err)
            stop(9)
         })

      await pipeline.load()

      await pipeline.start()

      protocol.ready()

   } catch (err) {
      log.error(err.stack)
      stop(1)
   }

   async function shutdown() {
      await pipeline.stop()
      stop()
   }

   async function stop(exitCode) {
      try {
         const messageProcessedMetric = await pipeline.getMessageProcessedMetric()
         const metrics = messageProcessedMetric.values.reduce((m, metric) => {
            m[metric.labels.kind] = metric.value
            return m
         }, {in: 0, acked: 0, nacked: 0, ignored: 0, rejected: 0})
         protocol.stopped({
            pipeline: pipelineConfig.name,
            metrics
         })
         process.exit(exitCode)
      } catch (err) {
         log.error(err)
      }
   }
}