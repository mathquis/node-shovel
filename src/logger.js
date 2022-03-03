const Colors = require('colors/safe')
const Winston = require('winston')
const { format, transports, addColors } = Winston
const { combine, timestamp, label, printf, colorize, splat } = format

const colorizer = colorize()

addColors({
   time: 'grey',
   category: 'bold',
})

const isTTY = process.stdout.isTTY

replaceRecursive = (text, pattern, func, max = 10) => {
   const newText = text.replace(pattern, func)
   if (max === 0 || newText === text) {
      return newText
   }
   return replaceRecursive(newText, pattern, func, max - 1)
}

const customFormat = format.combine(
   splat(),
   timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
   format((info) => {
      info.level = info.level.toUpperCase()
      info.category = ( info.category || 'log' ).replace(/(.)([A-Z])/g, (_, $1, $2) => {
         return $1 + '-' + $2.toLowerCase()
      }).toLowerCase()
      return info
   })(),
   isTTY ? colorize({ level: true }) : format(info => info)(),
   printf(info => {
      if (isTTY) {
         info.timestamp = colorizer.colorize('time', info.timestamp)
         info.category = colorizer.colorize('category', info.category || '-')
         if (typeof info.message === 'string') {
            info.message = replaceRecursive(info.message, /\(([^(]+?)\((.*?)\)(.*)\)/gs, (_, $1, $2, $3) => {
                  return '(' + $1 + $2 + $3 + ')'
               })
               .replace(/\((.*?)\)/g, (_, $1) => {
                  return Colors.grey(
                     '(' + $1.replace(/(.+?): *?(.+?)(, |$)/g, (_, $1, $2, $3) => {
                        if ($2.trim() === '') {
                           return $1 + $3
                        }
                        const value = $2
                           .trim()
                           .replace(/"/g, '\'')
                        return $1 + ': ' + Colors.yellow(value) + $3
                     }) + ')'
                  );
               })
               .replace(/"(.*?)"/g, (_, $1) => {
                  return Colors.cyan($1);
               })
         } else {
            info.message = '\n' + JSON.stringify(info.message, null, 2)
         }
      }
      return `${info.timestamp} ${info.level}${info.pipeline ? ` [${info.pipeline}][${info.worker}]` : ''} ${info.category}: ${info.message}${info.stack ? '\n' + info.stack : ''}`
   })
)

const consoleTransport = new transports.Console()

const Logger = Winston.createLogger({
   level: 'info',
   format: customFormat,
   defaultMeta: {service: 'logger'},
   transports: [
      consoleTransport
   ]
})

module.exports = {
   setServiceName: name => {
      Logger.defaultMeta.service = name
   },
   setLogLevel: level => {
      for ( transport in Logger.transports ) {
         Logger.transports[transport].level = level || 'info'
      }
   },
   child: meta => {
      return Logger.child(meta)
   },
   debug: (...args) => {
      return Logger.debug.apply(Logger, args)
   },
   info: (...args) => {
      return Logger.info.apply(Logger, args)
   },
   warn: (...args) => {
      return Logger.warn.apply(Logger, args)
   },
   error: (...args) => {
      return Logger.error.apply(Logger, args)
   }
}