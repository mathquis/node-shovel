import { stringify as Serializer } from 'csv-stringify'

export default node => {
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
      .onIn(async (message) => {
         const {delimiter, escape, quote, columns, encoding} = node.getConfig()

         const payload = await new Promise((resolve, reject) => {
            Serializer([message.content], {
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
         message.encode(payload)
         node.out(message)
      })
}