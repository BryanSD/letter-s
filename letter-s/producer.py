import json
import random
import socket
import sys
import time

from eventlet import GreenPool
from marconiclient import client


def get_production_information(conn):
    queue = conn.get_queue('openstack-producer-controller')

    messages = list(queue.get_messages())

    if len(messages) > 0:
        last_message = messages[-1]
        return last_message['body']['rate'], last_message['body']['ttl']
    else:
        return 10, 60


if __name__ == "__main__":
    if len(sys.argv) < 8:
        raise Exception(
            'Please provide: client_id, auth_url, user,',
            'key, endpoint, graphite-ip, graphite-port')

    conn = client.Connection(sys.argv[1],
                             sys.argv[2],
                             sys.argv[3],
                             sys.argv[4],
                             endpoint=sys.argv[5])
    conn.connect('_')
    queue = conn.get_queue('openstack-tasks')

    rate, ttl = get_production_information(conn)
    pool = GreenPool(rate)

    s = socket.socket()
    s.connect((sys.argv[6], int(sys.argv[7])))

    job_types = {0: 'prime', 1: 'fibonacci'}

    def post_work(ttl):
        job_type = random.randint(0, 1)
        start_value = random.randint(0, 1000)

        message = {"job_type": job_types[job_type],
            "start_value": start_value}

    start_time = time.time()
    while True:

    messages_created = 0
    message_reports = 0
    while True:
        job_type = random.randint(0, 1)
        start_value = random.randint(0, 1000)

        message = {"job_type": job_types[job_type],
                   "start_value": start_value}

        queue.post_message(json.dumps(message), ttl)
        messages_created += 1

        # TODO(bryansd): Remove
        time.sleep(1)

        time_marker = time.time()
        time_diff = time_marker - start_time
        if (time_diff >= 1):
            # Adjust for times greater than 1 second
            messages_adjusted = int((1 / time_diff) * messages_created)

            print messages_adjusted

            graphite_message = 'openstack.producer.worker.rate %d %d\n' % (
                messages_adjusted, int(time_marker))
            s.sendall(graphite_message)

            start_time = time_marker
            messages_created = messages_created - messages_adjusted
            message_reports += 1

        if message_reports % 5 == 0:
            print 'check queue'

    s.close()
