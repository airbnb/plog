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
		prefix = instance.get('prefix', 'plog.')
		suffix = instance.get('suffix', '')
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
				self.rate(prefix + name + suffix, val, tags=tags)
			def rate(name, val):
				self.gauge(prefix + name + suffix, val, tags=tags)

			rate('uptime',
				stats['uptime'])
			counter('exceptions',
				stats['exceptions'])
			counter('unhandled_objects',
				stats['unhandled_objects'])
			counter('udp_simple',
				stats['udp_simple_messages'])
			counter('holes.from_dead_port',
				stats['holes_from_dead_port'])
			counter('holes.from_new_message',
				stats['holes_from_new_message'])
			counter('invalid_checksum',
				sum(stats['v0_invalid_checksum']))
			counter('fragments',
				sum(stats['v0_fragments']))
			counter('missing_fragments',
				sum(sum(a) for a in stats['dropped_fragments']))
			counter('invalid_fragments',
				sum(sum(a) for a in stats['v0_invalid_fragments']))

			cache = stats['cache']
			counter('cache.evictions',
				cache['evictions'])
			counter('cache.hits',
				cache['hits'])
			counter('cache.miss',
				cache['misses'])

			kafka = stats['kafka']
			rate('kafka.messages',
				kafka['messageRate']['rate'][0])
			rate('kafka.dropped',
				kafka['droppedMessageRate']['rate'][0])
			rate('kafka.bytes',
				kafka['byteRate']['rate'][0])
			rate('kafka.resends',
				kafka['resendRate']['rate'][0])
			rate('kafka.failed_sends',
				kafka['failedSendRate']['rate'][0])
			rate('kafka.serialization_errors',
				kafka['serializationErrorRate']['rate'][0])

		finally:
			sock.close()
