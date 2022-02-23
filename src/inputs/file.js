const Path      = require('path')
const Readline  = require('readline')
const InputNode = require('../input')
const Tail      = require('tail').Tail

const START_POSITION_BEGINNING = 'beginning'
const START_POSITION_END = 'end'

class FileInput extends InputNode {

  get configSchema() {
    return {
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
      'encoding': {
        doc: '',
        format: String,
        default: 'utf-8'
      }
    }
  }

  async start() {
    const filePath = Path.resolve(this.pipelineConfig.path, this.getConfig('file'))

    const opts = {
      separator: this.getConfig('separator'),
      fromBeginning: this.getConfig('start_position') === START_POSITION_BEGINNING,
      encoding: this.getConfig('encoding'),
      follow: true,
      nLines: 0,
      flushAtEOF: false,
      logger: {
        info: (...data) => {
          this.log.debug(...data)
        },
        error: (...data) => {
          this.log.error(...data)
        }
      }
    }

    this.watcher = new Tail(filePath, opts)

    this.watcher
      .on('error', err => {
        this.log.error(err.stack)
      })
      .on('line', async line => {
        this.log.debug('Received line: %s (length: %d)', line, line.length)
        if ( line.length === 0 ) return
        this.in('[FILE]')
        const message = await this.decode(line)
        this.out(message)
      })

    await super.start()
    this.up()
  }

  async stop() {
    if ( this.watcher ) {
      this.watcher.unwatch()
    }
    this.down()
    await super.stop()
  }
}

module.exports = FileInput