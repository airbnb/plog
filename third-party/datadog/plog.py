# !/usr/bin/env python

from socket import socket, AF_INET, SOCK_DGRAM, error as socketError
from select import select
from json import loads
from numbers import Number

from checks import AgentCheck


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
        finally:
            sock.close()


        def rate(name, val):
            self.rate(prefix + name + suffix, val, tags=tags)

        def gauge(name, val):
            self.gauge(prefix + name + suffix, val, tags=tags)

        gauge('uptime',
              stats['uptime'])

        rate('udp_simple',
             stats['udp_simple_messages'])
        rate('udp_invalid_version',
             stats['udp_invalid_version'])
        rate('v0_invalid_type',
             stats['v0_invalid_type'])
        rate('v0_invalid_multipart_header',
             stats['v0_invalid_multipart_header'])
        rate('unknown_command',
             stats['unknown_command'])
        rate('v0_commands',
             stats['v0_commands'])
        rate('exceptions',
             stats['exceptions'])
        rate('unhandled_objects',
             stats['unhandled_objects'])
        rate('holes.from_dead_port',
             stats['holes_from_dead_port'])
        rate('holes.from_new_message',
             stats['holes_from_new_message'])

        rate('fragments',
             sum(stats['v0_fragments']))
        rate('invalid_checksum',
             sum(stats['v0_invalid_checksum']))

        rate('invalid_fragments',
             sum(sum(a) for a in stats['v0_invalid_fragments']))
        rate('missing_fragments',
             sum(sum(a) for a in stats['dropped_fragments']))

        defragmenter = stats['defragmenter']
        rate('defragmenter.evictions',
             defragmenter['evictions'])
        rate('defragmenter.hits',
             defragmenter['hits'])
        rate('defragmenter.miss',
             defragmenter['misses'])

        def flatsum(json):
            if isinstance(json, Number):
                return json
            elif isinstance(json, list):
                return sum(flatsum(o) for o in json)
            else:
                return 0

        def feed_handler_metrics(path, json):
            if isinstance(json, dict):
                for k in json:
                    feed_handler_metrics(path + '.' + k, json[k])
            else:
                existing = handler_metrics.get(path, 0)
                handler_metrics[path] = existing + flatsum(json)

        handlers = stats['handlers']
        handler_metrics = {}

        for handler in handlers:
            handler_name = handler['name']
            for metric_name in handler:
                if metric_name != 'name':
                    metric_path = handler_name + '.' + metric_name
                    feed_handler_metrics(metric_path, handler[metric_name])

        for path in handler_metrics:
            # TODO: rates instead of gauges through YAML config?
            # We can use rate(my.metric{*}) on Datadog's side for graphing, but alerts
            # do not support functions _yet_.
            gauge(path, handler_metrics[path])


if __name__ == '__main__':
    from time import sleep
    from pprint import PrettyPrinter

    pp = PrettyPrinter(indent=4)
    check, instances = PlogCheck.from_yaml('./plog.yaml')

    for instance in instances:
        # Run twice to grab rates, otherwise only gauges appear
        check.check(instance)
        sleep(1)
        check.check(instance)
        # We don't support events yet
        # pp.pprint(check.get_events())
        pp.pprint(check.get_metrics())
