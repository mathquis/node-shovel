const Cluster	= require('cluster')

if (Cluster.isMaster) {
	require('./master')()
} else {
	require('./worker')()
}