module.exports = node => {
   node.on('in', message => {
      node.out(message)
   })
}