const Path = require('path')
const File = require('fs')

const START_POSITION_BEGINNING = 'beginning'
const START_POSITION_END = 'end'

module.exports = node => {
   let watcher

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
         }
      })
      .on('start', async () => {
         const {file, start_position, encoding} = node.getConfig()

         const filePath = Path.resolve(node.pipelineConfig.path, file)

         let fileStat
         try {
            fileStat = await File.promises.stat(filePath)
         } catch (err) {
            throw err
         }

         let startPosition = node.getConfig('start_position')
         switch ( startPosition ) {
            case START_POSITION_BEGINNING:
               startPosition = 0
               break
            case START_POSITION_END:
               startPosition = fileStat.size
               break
         }

         let reader
         try {
            reader = File.createReadStream(filePath, {start: startPosition, encoding})
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
}