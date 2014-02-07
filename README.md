# plog

Send unboxed or multipart messages over UDP, or line-separated messages over TCP, and have them forwarded to Kafka 0.8.

## Getting started

    $ ./gladew run
    $ printf 'foo\nbar\nbaz'|socat -t0 - TCP:127.0.0.1:54321
    $ printf 'yipee!'|socat -t0 - UDP-DATAGRAM:127.0.0.1:54321

## Configuration

All configuration can be done through system properties.
The app settings are read using Typesafe Config.

### Kafka settings

Please refer to [Kafka's documentation](http://kafka.apache.org/08/configuration.html).

If unspecified in system properties, we will set those defaults:
- `serializer.class`: `kafka.serializer.StringEncoder`
- `metadata.broker.list`: `kafka.serializer.StringEncoder` (other values untested and probably broken).

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

        $ printf '\0\0kill'|socat -t0 - UDP-DATAGRAM:127.0.0.1:54321

- PING will cause the process to reply back with PONG. Trailing payload is sent back and can be used for request/reply matching.

        $ printf "\0\0PingFor$$\n\n"|socat - UDP-DATAGRAM:127.0.0.1:54321
        PONGFor17575
        
        $

- STAT is used to request statistics in UTF-8-encoded JSON. Per convention, the trailing payload should be used for politeness.

        $ printf "\0\0statistics please, gentle service"|socat - UDP-DATAGRAM:127.0.0.1:54321
        {"tcpMessages":0, "udpSimpleMessages":0, "udpInvalidVersion":0, "v0InvalidType":0, "unknownCommand":1, "v0Commands":520, "v0MultipartMessages":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}

#### Packet type 01: multipart message

- Bytes 02-03: unsigned, big-endian, 16-byte integer. Packet count for the message (between 0 for 1 packet and 65534).
- Bytes 04-05: unsigned, big-endian, 16-byte integer. Index of this packet in the message (between 0 for the first packet and 65534).
- Bytes 06-07: unsigned, big-endian, 16-byte integer. Byte length of the payload for each packet in the message.
- Bytes 08-11: arbitrary 32-byte integer. Second half of the identifier for the message. Messages are identified by the UDP client port and this second half.
- Bytes 12-15: signed, big-endian, 32-byte integer below 2,147,483,647. Total byte length of the message.
- Bytes 16-23: zeroes. Reserved, might be used in later revisions.
- Bytes 24-: bytes (UTF-8 by default). Payload. Will only read the payload length.

## Operational tricks

- To minimize packet loss due to "lacks", increase the kernel socket buffer size. For Linux, `sysctl net.core.rmem_max`.
