# shovel

## Installation

`npm i @mathquis/node-shovel`

## Usage

`shovel --pipeline pipeline.yml --config config.yml --workers 1 --log-level debug --metrics-port 3001`

### Prometheus metrics

Pipeline metrics are exposed as Prometheus format on the specified port

### Pipeline configuration

```yaml
name: message
workers: 2

input:
  use: amqp
  codec:
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
  codec:
  options:
    scheme: http
    index_name: audit-events
    index_shard: '{YYYY}' # YYYY, YY, MM, M, DD, D
    template: template.js
    username: elastic
    password: password
```

Pipeline configuration can use environment variables like so `${NAME:default}`.

### Available input

- amqp
- http
- udp (WIP)
- stdin

### Available output

- amqp
- elasticsearch
- stdout

### Codec

```javascript
module.exports = () => {
  return {
    decode: async content => {
      return content
    },
    encode: async message => {
      return message.content
    }
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