import Path from 'path'
import File from 'fs'
import Readline from 'readline'
import Zlib from 'zlib'
import Tail from 'tail'

const START_POSITION_BEGINNING = 'beginning'
const START_POSITION_END = 'end'

export default node => {
   let reader, latestPosition

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
         follow: {
            doc: '',
            format: Boolean,
            default: true
         },
         // separator: {
         //    doc: '',
         //    format: RegExp,
         //    default: /[\r]?\n/
         // },
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
      .onStart(async () => {
         const {file, separator, start_position, encoding, follow, gzip} = node.getConfig()

         const filePath = Path.resolve(node.pipelineConfig.path, file)

         if ( follow ) {
            if ( gzip ) {
               throw new Error('Cannot use "gzip" option with "follow"')
            }

            const opts = {
               // separator,
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

            reader = new Tail(filePath, opts)

            reader
               .on('line', onLine)

         } else {

            reader = File.createReadStream(filePath)

            reader
               .on('close', () => node.shutdown())

            const parser = Readline.createInterface({
               input: gzip ? reader.pipe(Zlib.createGunzip()) : reader
            })

            parser
               .on('error', err => node.error(err))
               .on('line', onLine)
         }

         // reader
         //    .on('error', err => node.error(err))

         node.up()
      })
      .onStop(async () => {
         if ( reader ) {
            if ( reader.unwatch ) {
               reader.unwatch()
            } else {
               reader.close()
            }
         }
      })
      .onPause(async () => {
         if ( reader ) {
            if ( reader.pause ) {
               reader.pause()
            }
         }
      })
      .onResume(async () => {
         if ( reader ) {
            if ( reader.resume ) {
               reader.resume()
            }
         }
      })

   function onLine(line) {
      setImmediate(() => {
         node.log.debug('Received line: %s (length: %d)', line, line.length)
         // process.stdout.write('.')
         const message = node.createMessage()
         message.source = line
         message.setContentType('text/plain')
         node.in(message)
      })
   }
}