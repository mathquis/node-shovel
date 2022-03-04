const Path = require('path')
const File = require('fs')

const START_POSITION_BEGINNING = 'beginning'
const START_POSITION_END = 'end'

module.exports = node => {
   let reader

   node
      .registerConfig({
         file: {
            doc: '',
            format: String,
            default: ''
         },
         start_position: {
            doc: '',
            format: [START_POSITION_BEGINNING, START_POSITION_END, Number],
            default: START_POSITION_BEGINNING
         },
         buffer_size: {
            doc: '',
            format: Number,
            default: 100000
         }
      })
      .on('start', async () => {
         const {file, start_position, encoding, buffer_size: highWaterMark} = node.getConfig()

         const filePath = Path.resolve(node.pipelineConfig.path, file)

         let fileStat
         try {
            fileStat = await File.promises.stat(filePath)
         } catch (err) {
            throw err
         }

         let startPosition = start_position
         switch ( startPosition ) {
            case START_POSITION_BEGINNING:
               startPosition = 0
               break
            case START_POSITION_END:
               startPosition = fileStat.size
               break
         }

         try {
            reader = File.createReadStream(filePath, {start: startPosition, encoding, highWaterMark})
            reader
               .on('data', payload => {
                  node.in(payload)
               })
               .on('error', err => {
                  node.error(err)
               })
               .on('end', () => {
                  node.down()
               })
         } catch (err) {
            node.error(err)
            return
         }

         node.up()
      })
      .on('stop', async () => {
         if ( reader ) {
            reader.destroy()
         }
      })
      .on('pause', () => {
         if ( reader ) {
            reader.pause()
         }
      })
      .on('resume', () => {
         if ( reader ) {
            reader.resume()
         }
      })
}