import json
import random
import socket
import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 8:
        raise Exception(
            'Please provide: client_id, auth_url, user,',
            'key, endpoint, graphite-ip, graphite-port')

    conn = client.Connection(sys.argv[1],
                             sys.argv[2],
                             sys.argv[3],
                             sys.argv[4],
                             endpoint=sys.argv[5],
                             token='_')
    conn.connect()

    queue = conn.get_queue('openstack-tasks')

    s = socket.socket()
    s.connect((sys.argv[6], int(sys.argv[7])))

    job_types = {0: 'prime', 1: 'fibonacci'}

    start_time = time.time()
    messages_created = 0
    while True:
        job_type = random.randint(0, 1)
        start_value = random.randint(0, 1000)

        message = {"job_type": job_types[job_type],
                   "start_value": start_value}

        queue.post_message(json.dumps(message), 60)
        messages_created += 1

        time_marker = time.time()
        time_diff = time_marker - start_time
        if (time_diff >= 1):
            # Adjust for times greater than 1 second
            messages_adjusted = (1 / time_diff) * messages_created

            graphite_message = 'openstack.producer.worker.rate %d %d\n' % (
                messages_adjusted, int(time_marker))
            s.sendall(graphite_message)

            start_time = time_marker
            messages_created = messages_created - messages_adjusted

    s.close()
