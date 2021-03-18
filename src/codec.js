module.exports = {
	// Data -> Data
	decode: content => content,
	// Message -> Data
	encode: message => message.content
}