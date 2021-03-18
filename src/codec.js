module.exports = {
	// Data -> Data
	decode: content => content,
	// Message -> content
	encode: message => message.content
}