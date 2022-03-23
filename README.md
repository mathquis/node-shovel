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


### Node

```javascript
export default node => {
  node
    // Use convict schema
    .registerConfig({})

    // Create a new message object
    .createMessage()

    // Events: start, stop, up, down, pause, resume, in, ou, ack, nack, ignore, reject, error
    .on(event, handler)
    .off(event, handler)
    .once(event, handler)

    // When the node starts (if set, the handler is responsible for calling node.up())
    .onStart(async () => {})

    // When the node stops
    .onStop(async () => {})

    // When the node is up (connected, ready, etc.)
    .onUp(async () => {})
    .up()

    // When the node is down (disconnected, unable to process messages)
    .onDown(async () => {})
    .down()

    // When the node should pause processing messages
    .onPause(async () => {})
    .pause()

    // When the node should resume processing messages
    .onResume(async () => {})
    .resume()

    // When the node receives a message
    .onIn(async (message) => {})
    .in(message)

    // When the node push a message down the pipeline
    .onOut(async (message) => {})
    .out(message)

    // When the node acks a message
    .onAck(async (message) => {})
    .ack(message)

    // When a node nacks a message
    .onUnack(async (message) => {})
    .unack(message)

    // When the node ignores a message
    .onIgnore(async (message) => {})
    .ignore(message)

    // When the node rejects a message
    .onReject(async (message) => {})
    .reject(message)

    // When the node triggers an error
    .error(err)
}
```

### Decoder

```javascript
export default node => {
  node
    .registerConfig({})
    .onIn(async (message) => {
      message.decode(decodedValue)
      node.out(message)
    })
}
```

### Encoder

```javascript
export default node => {
  node
    .registerConfig({})
    .onIn(async (message) => {
      message.encode(encodedValue)
      node.out(message)
    })
}
```

### Pipeline

```javascript
export default node => {
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
    .onIn(async (message) => {
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