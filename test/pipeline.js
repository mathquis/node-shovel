module.exports = () => {
	return async (message, next) => {
		next(null, [message])
	}
}