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
  options:
    vhost: '/'
    username: rabbitmq
    password: password
    exchange_name: exchange
    exchange_type: topic
    queue_name: queue
    bind_pattern: '#'

decoder:
  use: parser.js
  split: true
  options:

pipeline:
  use: pipeline.js
  options:

encoder:
  use: noop
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
- http-request
- http-server
- mqtt
- stdin
- stream
- tail
- tcp
- udp

### Available outputs

- amqp
- blackhole
- elasticsearch
- file
- mqtt
- stdout
- tcp
- udp

### Available codecs

- csv
- json
- json5
- multiline (WIP)
- protobuf
- syslog

### Custom decoder/encoder

```javascript
module.exports = node => {
  node
    .registerConfig({})
    .on('decode', async (message) => {
      node.out(message)
    })
    .on('encode', async (message) => {
      node.out(message)
    })
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

    .on('in', async (message) => {
      node.out(message)
    })
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