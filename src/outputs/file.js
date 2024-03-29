import Path from 'path'
import File from 'fs'

export default node => {
   let writer, draining

   node
      .registerConfig({
         file: {
            doc: '',
            format: String,
            default: ''
         },
         encoding: {
            doc: '',
            format: String,
            default: 'utf8'
         }
      })
      .onStart(async () => {
         const {file, encoding} = node.getConfig()

         const filePath = Path.resolve(node.pipelineConfig.path, file)

         try {
            writer = File.createWriteStream(filePath, {encoding})
         } catch (err) {
            node.error(err)
            return
         }

         node.log.info('Writing to "%s"', filePath)

         node.up()
      })
      .onStop(async () => {
         if ( writer ) {
            await new Promise((resolve, reject) => {
               writer.end('', 'binary', resolve)
            })
         }
      })
      .onIn(async (message) => {
         const result = writer.write(message.payload, () => {
            node.ack(message)
         })
         if ( !result && !draining ) {
            draining = true
            writer.once('drain', () => {
               node.resume()
            })
            node.nack(message)
            node.pause()
         }
      })
}