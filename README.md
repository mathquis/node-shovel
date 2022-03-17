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
  options:

pipeline:
  use: pipeline.js
  options:

encoder:
  use: noop
  options:

queue:
  use: batch
  options:
    persistent: true # Store queue on disk
    batch_size: 1000
    flush_timeout: 5s

output:
  use: elasticsearch
  options:
    scheme: http
    index_name: audit-events-{YYYY}-{MM}
    template: template.js
    username: elastic
    password: password
```

Pipeline configuration can use environment variables like so `${NAME:default}`.

### Available inputs

- amqp
- file
- http-request
- http-server
- mqtt
- noop
- stdin
- stream
- syslog
- tcp
- udp

### Available decoders (optional)

- base64
- csv
- json
- json5
- multiline (WIP)
- noop
- protobuf

### Available encoders (optional)

- base64
- csv
- format
- json
- json5
- noop
- protobuf

### Available queues (optional)

- batch

### Available outputs

- amqp
- blackhole
- debug
- elasticsearch
- file
- mqtt
- pipeline
- stdout
- tcp
- udp


### Custom decoder/encoder

```javascript
module.exports = node => {
  node
    .registerConfig({})
    .on('in', async (message) => {
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

    .on('pause', async () => {})
    .on('resume', async () => {})

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