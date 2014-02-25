from checks import AgentCheck
from socket import socket, AF_INET, SOCK_DGRAM, error as socketError
from select import select
from json import loads

class PlogCheck(AgentCheck):
	def check(self, instance):
		# read config
		tags = instance.get('tags', [])
		host = instance.get('host', '127.0.0.1')
		port = instance.get('port', 23456)
		timeout = instance.get('timeout', 3)
		max_size = instance.get('max_size', 65536)

		# create socket, ask for stats
		sock = socket(AF_INET, SOCK_DGRAM)
		try:
			sock.sendto("\0\0statfordatadogplease", (host, port))

			# wait for a reply
			ready = select([sock], [], [], timeout)
			if not ready[0]:
				raise socketError('timeout')

			data, addr = sock.recvfrom(max_size)
			stats = loads(data)

			def counter(name, val):
				self.rate(name, val, tags=tags)
			def rate(name, val):
				self.gauge(name, val, tags=tags)

			rate('plog.uptime',
				stats['uptime'])
			counter('plog.exceptions',
				stats['exceptions'])
			counter('plog.failed_to_send',
				stats['failed_to_send'])
			counter('plog.udp_simple',
				stats['udp_simple_messages'])
			counter('plog.holes.from_dead_port',
				stats['holes_from_dead_port'])
			counter('plog.holes.from_new_message',
				stats['holes_from_new_message'])
			counter('plog.invalid_checksum',
				sum(stats['v0_invalid_checksum']))
			counter('plog.fragments',
				sum(stats['v0_fragments']))
			counter('plog.missing_fragments',
				sum(sum(a) for a in stats['dropped_fragments']))
			counter('plog.invalid_fragments',
				sum(sum(a) for a in stats['v0_invalid_fragments']))

			cache = stats['cache']
			counter('plog.cache.evictions',
				cache['evictions'])
			counter('plog.cache.hits',
				cache['hits'])
			counter('plog.cache.miss',
				cache['misses'])

			kafka = stats['kafka']
			rate('plog.kafka.messages',
				kafka['messageRate']['rate'][0])
			rate('plog.kafka.dropped',
				kafka['droppedMessageRate']['rate'][0])
			rate('plog.kafka.bytes',
				kafka['byteRate']['rate'][0])
			rate('plog.kafka.resends',
				kafka['resendRate']['rate'][0])
			rate('plog.kafka.failed_sends',
				kafka['failedSendRate']['rate'][0])
			rate('plog.kafka.serialization_errors',
				kafka['serializationErrorRate']['rate'][0])

		finally:
			sock.close()
