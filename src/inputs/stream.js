import Path from 'path'
import File from 'fs'

const START_POSITION_BEGINNING = 'beginning'
const START_POSITION_END = 'end'

export default node => {
   let reader

   node
      .registerConfig({
         file: {
            doc: '',
            format: String,
            default: ''
         },
         gzip: {
            doc: '',
            format: Boolean,
            default: false
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
            reader = File.createReadStream(filePath, {start: startPosition, highWaterMark})

            node.log.info('Streaming "%s" (start: %d, buffer: %d)', filePath, startPosition, highWaterMark)

            reader
               .on('data', buffer => {
                  const message = node.createMessage()

                  message.source = buffer

                  node.in(message)
               })
               .on('error', err => {
                  node.error(err)
               })
               .on('end', () => {
                  node.shutdown()
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