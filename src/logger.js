const Winston                                     = require('winston')
const Config                                      = require('./config')
const {format, transports}                        = Winston
const {colorize, splat, timestamp, printf, align} = format
const colorizer                                   = colorize()
const isTTY                                       = process.stdout.isTTY

const logFmt = printf(info => {
  let paddedLevel = info.level.padStart(5, ' ')
  if ( isTTY ) {
    paddedLevel = colorizer.colorize(info.level, paddedLevel)
  }
  return `${info.timestamp} [${paddedLevel}] [${(info.pipeline || '').padEnd(16, ' ')} ${(info.worker ? info.worker : ' ' ).toString().padStart(3, ' ')}] ${info.category.padEnd(24, ' ')}: ${info.message}`
})

const customFormat = format.combine(
  splat(),
  timestamp(),
  logFmt,
)

const logger = Winston.createLogger({
  level: Config.get('log.level', 'info'),
  format: customFormat,
  transports: [
    new Winston.transports.Console({
      // stderrLevels: ['emerg', 'alert', 'crit', 'error', 'warning', 'notice', 'info', 'debug'] // All
    })
  ]
})

module.exports = {
  child: (options) => logger.child(options),
  setLevel: level => {
    logger.transports.forEach(transport => {
      transport.level = level
    })
  }
}