import socket
import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 5:
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

    headers = {'Client-ID': sys.argv[1]}

    s = socket.socket()
    s.connect((sys.argv[6], int(sys.argv[7])))

    start_time = time.time()
    messages_created = 0
    while True:
        queue.post_message('{"test": true}', 5)
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
