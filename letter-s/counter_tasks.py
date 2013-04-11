import socket
import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception(
            'Please provide: Marconi endpoint, Graphite IP, Graphite Port')

    conn = client.Connection('2',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=sys.argv[1])
    conn.connect('_')

    queue = conn.get_queue('openstack-tasks')

    s = socket.socket()
    s.connect((sys.argv[2], int(sys.argv[3])))

    start_time = time.time()
    messages_created = 0
    while True:
        message_count = len(list(queue.get_messages()))

        if message_count == 0:
            time.sleep(1)
            continue

        messages_created += message_count

        time_marker = time.time()
        time_diff = time_marker - start_time
        if (time_diff >= 1):
            # Adjust for times greater than 1 second
            messages_adjusted = int((1 / time_diff) * messages_created)

            graphite_message = 'openstack.queue.work.rate %d %d\n' % (
                messages_adjusted, int(time_marker))
            s.sendall(graphite_message)

            start_time = time_marker
            messages_created = messages_created - messages_adjusted

    s.close()
