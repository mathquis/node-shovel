const Table 		= require('cli-table')

const traverse = (obj, cb, lastKey) => {
  for (let k in obj) {
  	let key = lastKey
  	if ( k !== '_cvtProperties' ) {
    	key = `${lastKey ? lastKey + '.' : ''}${k}`
	    if ( !cb(key, obj[k]) ) {
	        continue
	    }
    }
    if (obj[k] && typeof obj[k] === 'object') {
      traverse(obj[k], cb, key)
    }
  }
}

module.exports = worker => {
	const table = new Table({
	    head: ['key', 'arg', 'env', 'default', 'doc']
	})
	traverse(worker.help(), (key, val) => {
		const hasDoc = typeof val.doc !== 'undefined'
		if ( hasDoc ) {
			const {arg, env, default: def, doc} = val
			table.push([key, arg || '', env || '', def || '', doc])
		}
		return !hasDoc
	})
	return table.toString()

}