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
  split: true
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

### Available inputs

- amqp
- file
- http
- mqtt
- stdin
- tcp
- udp

### Available outputs

- amqp
- elasticsearch
- mqtt
- stdout
- tcp
- udp

### Available codecs

- csv
- json
- syslog

### Custom codec

```javascript
module.exports = codec => {
  return {
    decode: async data => {
      return data
    },
    encode: async message => {
      return [message.content]
    }
  }
}
```

### Pipeline

```javascript
module.exports = node => {
  node
    .registerConfig({
      enabled: {
        doc: '',
        format: Boolean,
        default: true
      },
      blocked: {
        doc: '',
        format: Boolean,
        default: true
      }
    })
    .on('in', async (message) => {
      message.setId(1)
      message.setDate(new Date())

      const {blocked} = node.getConfig()

      if (blocked) {
        // Reject message
        node.reject(message)
      } else if (!node.getConfig('enabled')) {
        // Ignore message
        node.ignore(message)
      } else {
        // Process message
        node.out(message)
      }
    })
}
```

### Custom node

```javascript
module.exports = node => {
  node
    // Use convict schema
    .registerConfig({})

    .on('start', async () => {})
    .on('stop', async () => {})

    .on('up', async () => {})
    .on('down', async () => {})

    .on('in', async (message) => {})
    .on('out', async (message) => {})

    .on('ack', async (message) => {})
    .on('unack', async (message) => {})
    .on('ignore', async (message) => {})
    .on('reject', async (message) => {})

    .on('error', async (err) => {})
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