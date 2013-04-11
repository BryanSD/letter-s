import socket
import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception(
            'Please provide: Marconi endpoint, Graphite IP, Graphite Port')

    conn = client.Connection('3',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=sys.argv[1])
    conn.connect('_')

    queue = conn.get_queue('openstack-responses')

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

        print messages_created

        graphite_message = 'openstack.queue.result.sum %d %d\n' % (
            messages_created, int(time.time()))
        s.sendall(graphite_message)

    s.close()
