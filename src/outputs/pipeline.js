const META_ORIGIN_PIPELINE = 'output-pipeline-origin'

export default node => {

   node
      .registerConfig({
         pipelines: {
            doc: '',
            format: Array,
            default: []
         },
         mode: {
            doc: '',
            format: ['broadcast', 'fanout'],
            default: 'fanout'
         }
      })
      .on('in', async (message) => {
         const {pipelines, mode} = node.getConfig()
         message.setHeader(META_ORIGIN_PIPELINE, node.pipelineConfig.name)
         switch ( mode ) {
            case 'broadcast':
               node.broadcast(pipelines, message)
               break
            case 'fanout':
               node.fanout(pipelines, message)
               break
         }
         node.ack(message)
      })
}