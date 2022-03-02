const Parser = require('csv-parse').parse
const Serializer = require('csv-stringify').stringify

module.exports = node => {
   node
      .registerConfig({
         delimiter: {
            doc: '',
            format: String,
            default: ','
         },
         escape: {
            doc: '',
            format: String,
            default: '"'
         },
         quote: {
            doc: '',
            format: String,
            default: '"'
         },
         columns: {
            doc: '',
            format: Array,
            default: []
         },
         encoding: {
            doc: '',
            format: ['utf8', 'ucs2', 'utf16le', 'latin1', 'ascii', 'base64', 'hex'],
            default: 'utf8'
         }
      })
      .on('decode', async (message) => {
         const {delimiter, escape, quote, columns, encoding} = node.getConfig()

         message.content = await new Promise((resolve, reject) => {
            Parser(message.payload, {
               delimiter,
               escape,
               quote,
               columns: columns.length > 0 ? columns : false,
               encoding
            }, (err, records) => {
               if (err) {
                  reject(err)
                  return
               }
               resolve(records)
            })
         })
         node.out(message)
      })
      .on('encode', async (message) => {
         const {delimiter, escape, quote, columns, encoding} = node.getConfig()

         message.payload = await new Promise((resolve, reject) => {
            Serializer(message.content, {
               delimiter,
               escape,
               quote,
               columns: columns.length > 0 ? columns : false,
               encoding
            }, (err, output) => {
               if (err) {
                  reject(err)
                  return
               }
               resolve(output)
            })
         })

         node.out(message)
      })
}