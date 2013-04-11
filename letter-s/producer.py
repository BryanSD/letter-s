import random
import socket
import sys
import time

import eventlet
from eventlet import GreenPool
from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception(
            'Please provide: Marconi endpoint, Graphite IP, Graphite Port')

    conn = client.Connection('1',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=sys.argv[1])
    conn.connect('_')

    pool = GreenPool()

    rate, ttl = 0, 60
    messages_created = 0

    def get_production_information():
        global rate, ttl
        queue = conn.get_queue('openstack-producer-controller')

        while True:
            messages = list(queue.get_messages(restart=True))

            if len(messages) > 0:
                last_message = messages[-1]
                rate = last_message['body']['rate']
                ttl = last_message['body']['ttl']
            else:
                rate, ttl = 10, 60

            eventlet.sleep(1)

    pool.spawn_n(get_production_information)

    def post_work():
        global rate, ttl, messages_created

        queue = conn.get_queue('openstack-tasks')
        job_types = {0: 'prime', 1: 'fibonacci'}

        while True:
            if rate == 0:
                eventlet.sleep(1)
                continue
            else:
                eventlet.sleep(1.0 / rate)

            job_type = random.randint(0, 1)
            start_value = random.randint(0, 1000)

            message = {"job_type": job_types[job_type],
                       "start_value": start_value}

            queue.post_message(message, ttl)
            messages_created += 1

    pool.spawn_n(post_work)

    def post_stats():
        global messages_created

        s = socket.socket()
        s.connect((sys.argv[2], int(sys.argv[3])))

        while True:
            graphite_message = 'openstack.producer.worker.rate %d %d\n' % (
                messages_created, int(time.time()))
            s.sendall(graphite_message)

            messages_created = 0

            eventlet.sleep(1)

        s.close()

    pool.spawn_n(post_stats)

    pool.waitall()
