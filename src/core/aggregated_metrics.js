'use strict';

/**
 * Extends the Registry class with a `clusterMetrics` method that returns
 * aggregated metrics for all workers.
 *
 * In cluster workers, listens for and responds to requests for metrics by the
 * cluster master.
 */

import Cluster from 'cluster';
import Registry from 'prom-client/lib/registry.js'
import { Grouper } from 'prom-client/lib/util.js'
import { aggregators } from 'prom-client/lib/metricAggregators.js'

const GET_METRICS_REQ = 'agg_metrics:getMetricsReq';
const GET_METRICS_RES = 'agg_metrics:getMetricsRes';

let registries = [Registry.globalRegistry];
let requestCtr = 0; // Concurrency control
let listenersAdded = false;
const requests = new Map(); // Pending requests for workers' local metrics.

export default class AggregatorRegistry extends Registry {
   constructor() {
      super();
      addListeners();
   }

   /**
    * Gets aggregated metrics for all workers. The optional callback and
    * returned Promise resolve with the same value; either may be used.
    * @return {Promise<string>} Promise that resolves with the aggregated
    *   metrics.
    */
   clusterMetrics(options) {
      const requestId = requestCtr++;

      let additionRegistries = []
      if ( options && options.registries ) {
         additionRegistries = options.registries
      }

      return Promise
         .all(additionRegistries.map(r => r.getMetricsAsJSON()))
         .then(responses => {
            return new Promise((resolve, reject) => {
               let settled = false;
               function done(err, result) {
                  if (settled) return;
                  settled = true;
                  if (err) reject(err);
                  else resolve(result);
               }

               const request = {
                  responses,
                  pending: 0,
                  done,
                  errorTimeout: setTimeout(() => {
                     const err = new Error('Operation timed out.');
                     request.done(err);
                  }, 5000),
               };

               requests.set(requestId, request);

               const message = {
                  type: GET_METRICS_REQ,
                  requestId,
               };

               for (const id in Cluster.workers) {
                  // If the worker exits abruptly, it may still be in the workers
                  // list but not able to communicate.
                  if (Cluster.workers[id].isConnected()) {
                     Cluster.workers[id].send(message);
                     request.pending++;
                  }
               }

               if (request.pending === 0) {
                  // No workers were up
                  clearTimeout(request.errorTimeout);
                  process.nextTick(() => done(null, ''));
               }
            });
      })
   }

   /**
    * Creates a new Registry instance from an array of metrics that were
    * created by `registry.getMetricsAsJSON()`. Metrics are aggregated using
    * the method specified by their `aggregator` property, or by summation if
    * `aggregator` is undefined.
    * @param {Array} metricsArr Array of metrics, each of which created by
    *   `registry.getMetricsAsJSON()`.
    * @return {Registry} aggregated registry.
    */
   static aggregate(metricsArr) {
      const aggregatedRegistry = new Registry();
      const metricsByName = new Grouper();

      // Gather by name
      metricsArr.forEach(metrics => {
         metrics.forEach(metric => {
            metricsByName.add(metric.name, metric);
         });
      });

      // Aggregate gathered metrics.
      metricsByName.forEach(metrics => {
         const aggregatorName = metrics[0].aggregator;
         const aggregatorFn = aggregators[aggregatorName];
         if (typeof aggregatorFn !== 'function') {
            throw new Error(`'${aggregatorName}' is not a defined aggregator.`);
         }
         const aggregatedMetric = aggregatorFn(metrics);
         // NB: The 'omit' aggregator returns undefined.
         if (aggregatedMetric) {
            const aggregatedMetricWrapper = Object.assign(
               {
                  get: () => aggregatedMetric,
               },
               aggregatedMetric,
            );
            aggregatedRegistry.registerMetric(aggregatedMetricWrapper);
         }
      });

      return aggregatedRegistry;
   }

   /**
    * Sets the registry or registries to be aggregated. Call from workers to
    * use a registry/registries other than the default global registry.
    * @param {Array<Registry>|Registry} regs Registry or registries to be
    *   aggregated.
    * @return {void}
    */
   static setRegistries(regs) {
      if (!Array.isArray(regs)) regs = [regs];
      regs.forEach(reg => {
         if (!(reg instanceof Registry)) {
            throw new TypeError(`Expected Registry, got ${typeof reg}`);
         }
      });
      registries = regs;
   }
}

/**
 * Adds event listeners for cluster aggregation. Idempotent (safe to call more
 * than once).
 * @return {void}
 */
function addListeners() {
   if (listenersAdded) return;
   listenersAdded = true;

   if (Cluster.isMaster) {
      // Listen for worker responses to requests for local metrics
      Cluster.on('message', (worker, message) => {
         if (message.type === GET_METRICS_RES) {
            const request = requests.get(message.requestId);

            if (message.error) {
               request.done(new Error(message.error));
               return;
            }

            message.metrics.forEach(registry => request.responses.push(registry));
            request.pending--;

            if (request.pending === 0) {
               // finalize
               requests.delete(message.requestId);
               clearTimeout(request.errorTimeout);

               const registry = AggregatorRegistry.aggregate(request.responses);

               // const promString = registry.metrics();
               request.done(null, registry);
            }
         }
      });
   }
}

// Respond to master's requests for worker's local metrics.
process.on('message', message => {
   if (Cluster.isWorker && message.type === GET_METRICS_REQ) {
      Promise.all(registries.map(r => r.getMetricsAsJSON()))
         .then(metrics => {
            process.send({
               type: GET_METRICS_RES,
               requestId: message.requestId,
               metrics,
            });
         })
         .catch(error => {
            process.send({
               type: GET_METRICS_RES,
               requestId: message.requestId,
               error: error.message,
            });
         });
   }
});