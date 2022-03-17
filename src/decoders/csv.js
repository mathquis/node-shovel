import parse as Parser from 'csv-parse'

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
      .on('in', async (message) => {
         const {delimiter, escape, quote, columns, encoding} = node.getConfig()

         const content = await new Promise((resolve, reject) => {
            Parser(message.source, {
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
               resolve(records.length === 1 ? records[0] : records)
            })
         })
         message.decode(content)
         node.out(message)
      })
}