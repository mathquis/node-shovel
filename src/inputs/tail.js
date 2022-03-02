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
         },
         follow: {
            doc: '',
            format: Boolean,
            default: true
         }
      })
      .on('start', async () => {
         const {file, separator, start_position, encoding, follow} = node.getConfig()

         const filePath = Path.resolve(node.pipelineConfig.path, file)

         const opts = {
            separator,
            fromBeginning: start_position === START_POSITION_BEGINNING,
            encoding,
            follow,
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
               node.in(line)
            })

         node.up()
      })
      .on('stop', async () => {
         if ( watcher ) {
            watcher.unwatch()
         }
      })
}