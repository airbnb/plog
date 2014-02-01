# plog

Send unboxed messages over UDP, or line-separated over TCP, and have them forwarded to Kafka.

## Usage

$ mvn package && java -jar target/plog-1.0-SNAPSHOT.jar
$ printf 'foo\nbar\nbaz'|socat -t0 - TCP:127.0.0.1:54321
$ printf 'yipee!'|socat -t0 - UDP-DATAGRAM:127.0.0.1:54321
