# shovel

## Installation

`npm i @mathquis/node-shovel`

## Usage

`shovel --pipeline pipeline.yml --config config.yml --workers 1 --log-level debug`

### Pipeline configuration

```yaml
name: message

input:
  use: amqp
  parser:
    use: parser.js
    options:
  options:
    vhost: '/'
    username: rabbitmq
    password: password
    exchange_name: exchange
    exchange_type: topic
    queue_name: queue
    bind_pattern: '#'

pipeline:
  use: pipeline.js
  options:

output:
  use: elasticsearch
  options:
    scheme: http
    index_name: audit-events
    index_shard: '{YYYY}'
    template: template.js
    username: elastic
    password: password
```

### Parser

```javascript
module.exports = () => {
	return async content => {
		return content
	}
}
```

### Pipeline

```javascript
module.exports = () => {
	return async (message, next) => {
		message.setId(1)
		message.setDate(new Date())

		// Ignore message
		next()

		// Reject message
		next(new Error('rejected'))

		// Process message
		next(null, [message])
	}
}
```

### Template

```javascript
module.exports = config => {
	return {
		"name": "template-name",
		"template": {
			"index_patterns": "template-*"],
			"settings": {
				"index": {
					"number_of_shards": 1,
					"refresh_interval": "10s"
				}
			},
			"aliases": {
				"template": {}
			},
			"mappings": {}
		}
	}
}
```