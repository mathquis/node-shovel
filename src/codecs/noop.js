module.exports = () => {
	return {
		// Data -> Data
		decode: content => content,
		// Message -> Data
		encode: message => message.content
	}
}