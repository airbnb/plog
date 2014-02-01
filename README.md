# plog

Send unboxed messages over UDP, or line-separated messages over TCP, and have them forwarded to Kafka 0.8.

## Getting started

    $ mvn package && java -jar target/plog-1.0-SNAPSHOT.jar
    $ printf 'foo\nbar\nbaz'|socat -t0 - TCP:127.0.0.1:54321
    $ printf 'yipee!'|socat -t0 - UDP-DATAGRAM:127.0.0.1:54321

## Configuration

All done through system properties.

### plog.max_line_length (default: 1048576 = 1MB)

Maximum number of bytes in a line. TCP only.

### plog.port (default: 54321)

Port to listen on, for TCP and UDP.

### plog.charset (default: UTF-8)

Charset for text. We use string serialization by default and the string value as a key for sharding.

### plog.topic (default: flog)

The Kafka topic we will send messages to.

### All Kafka settings

Also read from system properties. Please refer to [Kafka's documentation](http://kafka.apache.org/08/configuration.html).

If unspecified in system properties, we will set those defaults:
- `serializer.class`: `kafka.serializer.StringEncoder`
- `metadata.broker.list`: `kafka.serializer.StringEncoder` (other values untested and probably broken).
