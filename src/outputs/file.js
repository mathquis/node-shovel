const Path = require('path')
const File = require('fs')

module.exports = node => {
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
      .on('start', async () => {
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
      .on('stop', async () => {
         if ( writer ) {
            await new Promise((resolve, reject) => {
               writer.end('', 'binary', resolve)
            })
         }
      })
      .on('in', async (message) => {
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