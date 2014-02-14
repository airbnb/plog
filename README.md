# plog

Fire and forget unboxed or multipart messages over UDP, have them forwarded to Kafka 0.8.

## Disclaimer

**As-is:** This project is not actively maintained or supported.
While updates may still be made and we welcome feedback, keep in mind we may not respond to pull requests or issues quickly.

**Let us know!** If you fork this, or if you use it, or if it helps in anyway, we'd love to hear from you! opensource@airbnb.com


## Getting started

    $ ./gladew run
    $ printf 'yipee!'|socat -t0 - UDP-DATAGRAM:127.0.0.1:23456

## Configuration

All configuration can be done through system properties.
The app settings are read using Typesafe Config.

### Kafka settings

Please refer to [Kafka's documentation](http://kafka.apache.org/08/configuration.html).

If unspecified in system properties, we will set those defaults:
- `metadata.broker.list`: `127.0.0.1:9092` (other values untested and probably broken).
- `client.id`: `plog_$HOSTNAME`

### plog settings

Please refer to [reference.conf](src/main/resources/reference.conf) for the options and their default values.

## UDP protocol

- If the first byte is outside of the 0-31 range, the message is considered to be unboxed and the whole packet is parsed as a string.

- Otherwise, the first byte indicates the protocol version. Currently, only version `00` is defined.

### Version 00

- Byte 00: version (00)
- Byte 01: packet type


#### Packet type 00

Command packet. Commands are always 4 ASCII characters, trailing payload can be used. Command matching is case-insensitive.

- KILL crashes the process without any attention for detail or respect for ongoing operations.

        $ printf '\0\0kill'|socat -t0 - UDP-DATAGRAM:127.0.0.1:23456

- PING will cause the process to reply back with PONG. Trailing payload is sent back and can be used for request/reply matching.

        $ printf "\0\0PingFor$$\n\n"|socat - UDP-DATAGRAM:127.0.0.1:23456
        PONGFor17575
        
        $

- STAT is used to request statistics in UTF-8-encoded JSON. Per convention, the trailing payload should be used for politeness.

        $ printf "\0\0statistics please, gentle service"|socat - UDP-DATAGRAM:127.0.0.1:23456
        {""udpSimpleMessages":0, [...]}

- ENVI returns the environment as a UTF-8-encoded string. The format is not defined further.

#### Packet type 01: multipart message

- Bytes 02-03: unsigned, big-endian, 16-bit integer. Packet count for the message (between 1 and 65535).
- Bytes 04-05: unsigned, big-endian, 16-bit integer. Index of this packet in the message (between 0 for the first packet and 65534).
- Bytes 06-07: unsigned, big-endian, 16-bit integer. Byte length of the payload for each packet in the message.
- Bytes 08-11: arbitrary 32-byte integer. Second half of the identifier for the message. Messages are identified by the UDP client port and this second half.
- Bytes 12-15: signed, big-endian, 32-bit integer below 2,147,483,647. Total byte length of the message.
- Bytes 16-19: big-endian, 32-bit MurmurHash3 hash of the total message payload.
- Bytes 20-23: zeroes. Reserved, might be used in later revisions.
- Bytes 24-: bytes (UTF-8 by default). Payload. Will only read the payload length.

## Building a fat JAR

    $ ./gradlew shadowJar
    $ ls -l ./build/distributions/plog-1.0-SNAPSHOT-shadow.jar

## Operational tricks

- To minimize packet loss due to "lacks", increase the kernel socket buffer size. For Linux, we use `sysctl net.core.rmem_max = 1048576`.

## Event logging at Airbnb

We use JSON objects with the following fields:

- type: String. Only very few values are acceptable due to our pipeline splitting event streams by type.
- uuid: String.
- host: String.
- timestamp: int64.
- data: String→String map or String.
