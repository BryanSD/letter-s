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

    errors_detected = 0

    def get_production_information():
        global rate, ttl
        queue = conn.get_queue('openstack-producer-controller')

        while True:
            try:
                messages = list(queue.get_messages())

                if len(messages) > 0:
                    last_message = messages[-1]
                    rate = int(last_message['body']['rate'])
                    ttl = int(last_message['body']['ttl'])

                    print 'New control message: rate=%d, ttl=%d' % (rate, ttl)
            except Exception as ex:
                print ex

            eventlet.sleep(1)

    pool.spawn_n(get_production_information)

    def async_post_message(queue, message):

        global ttl

        try:
            msg = queue.post_message(message, ttl)
        except Exception as ex:
            msg = None

        return msg

    def on_message_posted(gthread):
        global messages_created, errors_detected
        msg = gthread.wait()

        if msg:
            messages_created += 1
        else:
            errors_detected += 1


    def post_work():
        global rate, ttl, messages_created

        queue = conn.get_queue('openstack-tasks')
        job_types = {0: 'prime', 1: 'fibonacci'}

        while True:
            if rate == 0:
                eventlet.sleep(1)
                continue

            unadjusted_sleep_time = (1.0 / rate)
            start_time = time.time()

            job_type = random.randint(0, 1)
            start_value = random.randint(0, 100)

            message = {"job_type": job_types[job_type],
                       "start_value": start_value}

            gt = pool.spawn(async_post_message, queue, message)
            gt.link(on_message_posted)

            elapsed_time = time.time() - start_time
            sleep_time = unadjusted_sleep_time - elapsed_time
            eventlet.sleep(max(sleep_time, 0))

    pool.spawn_n(post_work)

    def post_stats():
        global messages_created, errors_detected

        s = socket.socket()
        s.connect((sys.argv[2], int(sys.argv[3])))

        while True:
            start_time = time.time()

            graphite_message = 'openstack.producer.worker.rate %d %d\n' % (
                messages_created, int(time.time()))

            messages_created = 0
            errors_detected = 0

            s.sendall(graphite_message)

            eventlet.sleep(1 - (time.time() - start_time))

        s.close()

    pool.spawn_n(post_stats)

    pool.waitall()
