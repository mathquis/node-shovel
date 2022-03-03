const Path = require('path')
const File = require('fs')

module.exports = node => {
   let writer

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

         node.up()
      })
      .on('stop', async () => {
         if ( writer ) {
            writer.end()
         }
      })
      .on('in', async (message) => {
         writer.write(message.payload)
      })
}