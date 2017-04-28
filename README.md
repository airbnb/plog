[![Build Status](https://travis-ci.org/airbnb/plog.png?branch=master)](https://travis-ci.org/airbnb/plog)
[![Download](https://api.bintray.com/packages/airbnb/jars/plog/images/download.png)](https://bintray.com/airbnb/jars/plog/_latestVersion)

# plog

Fire and forget unboxed or fragmented messages over UDP or TCP, have them forwarded
to Kafka 0.8 or the standard output. That should cover `syslog`.

## Disclaimer

**As-is:** This project is not actively maintained or supported.
While updates may still be made and we welcome feedback, keep in mind we may not respond to pull requests or issues quickly.

**Let us know!** If you fork this, or if you use it, or if it helps in anyway, we'd love to hear from you! opensource@airbnb.com

## Getting started

    $ printf 'plog.server.udp.listeners=[{handlers=[{provider="com.airbnb.plog.console.ConsoleOutputProvider"}]}]' > plog-distro/src/main/resources/application.conf
    $ ./gradlew run
    $ printf 'yipee!' | socat -t0 - UDP-DATAGRAM:127.0.0.1:23456
    $ printf '\0\0statsplz' | socat - UDP-DATAGRAM:127.0.0.1:23456

## Build / Upload

- To build a shadow JAR:

        $ ./gradlew shadowJar

- To build source JARs and upload to bintray manually:
  - Create an account on https://bintray.com (you need to create and register your own org first)
  - Ask an admin (e.g. Alexis Midon @alexism) to invite your newly created user to Airbnb's account
  - find your bintray API key (Edit Profile > API Key) and set BINTRAY_USER and BINTRAY_KEY environment variables locally. e.g.
  
        $ export BINTRAY_USER=<your_user_id>
        $ export BINTRAY_KEY=<your_api_key>
        
  - Tag master with a newer version. Use `git describe --tags` to see the previous version. e.g.
 
        $ git describe --tags --dirty
        v4.0.0-BETA-36-g21add12
    
    This means the previous tag was v4.0.0-BETA, there's been 36 commits since the tagged commit, and HEAD is at 21add12.
    We use com.github.ben-manes.versions to apply set the build version to the output of 'git describe --tags --dirty'

    To tag a new version, do something like the following
    
        $ git tag -a v4.0.1
        $ git describe --tags --dirty
        v4.0.1
        $ git push origin v4.0.1
 
  - Now you are ready to upload to bintray, by running the following command in the root plog directory
    
        $ ./gradlew build sourcesJar bintrayUpload

## Configuration

All configuration can be done through system properties.
The app settings are read using Typesafe Config.

### Kafka settings

Please refer to [Kafka's documentation](http://kafka.apache.org/08/configuration.html).

### plog settings

Please refer to [reference.conf](plog-api/src/main/resources/reference.conf) for all the options and their default values.

Note that multiple TCP and UDP ports can be configure and have separate settings, and each has their own sink
(whether Kafka or standard output).

## Building a fat JAR

    $ ./gradlew shadowJar
    $ ls -l plog-distro/build/libs/*-all.jar

## Operational tricks

- To minimize packet loss due to "lacks", increase the kernel socket buffer size.
  For Linux, we use `sysctl net.core.rmem_max = 4194304`
  and configure `plog.udp.defaults.SO_RCVBUF` accordingly.

- Hole detection is a bit difficult to explain, but worth looking into (the tests should help).
  It is enabled by default, but can be disabled for performance.

## Event logging at Airbnb

We use JSON objects with the following fields:

- `type`: String. Only very few values are acceptable due to our pipeline splitting event streams by type.
- `uuid`: String.
- `host`: String.
- `timestamp`: Number. Milliseconds since Epoch.
- `data`: Object. Arbitrary.

## Statistics

Let's go through all keys in the JSON object exposed by the `STAT` command:

- `version`: the current version if available from the JAR manifest, or `unknown`
- `failed_to_send`: number of times Kafka threw `FailedToSendMessageException` back
- `exceptions`: number of unhandled exceptions.
- `udp_simple_messages`: number of unboxed UDP messages received.
- `udp_invalid_version`: number of UDP messages with version between 1 and 31.
- `v0_invalid_type`: number of UDP messages using version 0 of the protocol and a wrong packet type.
- `unknown_command`: number of commands received that aren't known (eg `KLIL` instead of `KILL`).
- `v0_commands`: number of *valid* commands received.
- `v0_invalid_multipart_header`: number of `v0` fragments received with invalid headers (could not be parsed).
- `v0_fragments` (array): count of fragments received, whether valid or not,
  clustered by log2 of their index.
  *Ie*, the first number indicates how many first packets we've received,
  the second number how many second,
  the third number how many 3rd and 4th,
  the fourth how many 5th, 6th, 7th, 8th, etc.
- `v0_invalid_fragments` (array of arrays): count of invalid fragments received,
  clustered first by log2 of (their message's size - 1), then by their fragment index.
  A fragment is considered invalid if:
  - its header provides a fragment size, fragment count, or checksum that doesn't match the values
    from the first fragment we processed
  - its length is incorrect: the payload length should always match the fragment size provided in
    the first fragment we processed, unless they're at the end of the message, in which case they
    should exactly match the message length provided in the first fragment we processed.
- `v0_invalid_checksum` (array): count of messages received where the MurmurHash3 did not match the
  payload, clustered by log2 of (their fragment count - 1).
- `dropped_fragments` (array of arrays): count of fragments we expected to
  receive but didn't before evicting them from the defragmenter,
  clustered first by log2 of (their message's size - 1), then by their fragment index.
- `cache` (object):
  - `evictions`: count of yet-to-be-completed messages were evicted from the cache,
    either because they expired or we needed to make room for new entries
    (see `defrag.max_size` and `defrag.expire_time` in the config).
  - `hits`: how many times we tried to add fragments to an already known message.
    Note that this operation will fail for invalid fragments.
  - `misses`: how many times we received fragments for a message that we didn't know about yet
    (we don't hit the cache for single-fragment messages).
- `kafka` (object):
  - Keys: `byteRate`, `messageRate`, `failedSendRate`, `resendRate`, `droppedMessageRate`, `serializationErrorRate`
  - Values: objects with keys `count` and `rate`, an array offering 1-min, 5-min and 15-min rates.

## TCP protocol

Line-by-line separated, lines starting with `\0` are reserved.

## UDP protocol

- If the first byte is outside of the 0-31 range, the message is considered to be unboxed
  and the whole packet is parsed as a string.

- Otherwise, the first byte indicates the protocol version. Currently, only version `00` is defined.

### Version 00

- Byte 00: version (00)
- Byte 01: packet type


#### Packet type 00: commands

Command packet. Commands are always 4 ASCII characters, trailing payload can be used. Command matching is case-insensitive.

- `KILL` crashes the process without any attention for detail or respect for ongoing operations.

        $ printf '\0\0kill'|socat -t0 - UDP-DATAGRAM:127.0.0.1:23456

- `PING` will cause the process to reply back with PONG.
  Trailing payload is sent back and can be used for request/reply matching.

        $ printf "\0\0PingFor$$\n\n"|socat - UDP-DATAGRAM:127.0.0.1:23456
        PONGFor17575
        
        $

- `STAT` is used to request statistics in UTF-8-encoded JSON.
  By convention, the trailing payload should be used for politeness.

        $ printf "\0\0statistics please, gentle service"|socat - UDP-DATAGRAM:127.0.0.1:23456
        {""udpSimpleMessages":0, [...]}

- `ENVI` returns the environment as a UTF-8-encoded string.
  The format is not defined further.

#### Packet type 01: fragmented message

Note that 1-fragment fragmented messages are perfectly possible.

- Bytes 02-03: unsigned, big-endian, 16-bit integer. Fragment count for the message (between 1 and 65535).
- Bytes 04-05: unsigned, big-endian, 16-bit integer. Index of this fragment in the message (between 0 for the first fragment and 65534).
- Bytes 06-07: unsigned, big-endian, 16-bit integer. Byte length of the payload for each fragment in the message.
- Bytes 08-11: big-endian, 32-bit integer. Second half of the identifier for the message.
               Messages are identified by the UDP client port and this second half.
               Needs to increment with each message for hole detection.
- Bytes 12-15: signed, big-endian, 32-bit integer below 2,147,483,647. Total byte length of the message.
- Bytes 16-19: big-endian, 32-bit MurmurHash3 hash of the total message payload.
- Bytes 20-21: unsigned, big-endian, 16-bit integer. `taglength`: Size used to represent tags.
- Bytes 22-23: zeroes. Reserved, might be used in later revisions.
- Bytes 24-(24+taglength): Bytes. List of tags (`\0`-separated UTF-8 strings; can be `\0`-terminated or not).
- Bytes (24+taglength)-: Bytes. Payload. Will only read the payload length.
