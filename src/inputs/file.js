const Path      = require('path')
const Readline  = require('readline')
const Tail      = require('tail').Tail

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
         separator: {
            doc: '',
            format: RegExp,
            default: /[\r]?\n/
         },
         start_position: {
            doc: '',
            format: [START_POSITION_BEGINNING, START_POSITION_END],
            default: START_POSITION_END
         },
         encoding: {
            doc: '',
            format: String,
            default: 'utf-8'
         }
      })
      .on('start', async () => {
         const {file, separator, start_position, encoding} = node.getConfig()


         const filePath = Path.resolve(node.pipelineConfig.path, file)

         const opts = {
            separator,
            fromBeginning: start_position === START_POSITION_BEGINNING,
            encoding,
            follow: true,
            nLines: 0,
            flushAtEOF: false,
            logger: {
               info: (...data) => {
                  node.log.debug(...data)
               },
               error: (...data) => {
                  node.log.error(...data)
               }
            }
         }

         watcher = new Tail(filePath, opts)

         watcher
            .on('error', err => {
               node.log.error(err.stack)
            })
            .on('line', async line => {
               node.log.debug('Received line: %s (length: %d)', line, line.length)
               if ( line.length === 0 ) return
               node.in()
               try {
                  const messages = await node.decode(line)
                  messages.forEach(message => {
                     node.out(message)
                  })
               } catch (err) {
                  node.error(err)
                  node.reject()
               }
            })

         node.up()
      })
      .on('stop', async () => {
         if ( watcher ) {
            watcher.unwatch()
         }
      })
}