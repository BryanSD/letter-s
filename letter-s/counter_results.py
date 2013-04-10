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

    queue = conn.get_queue('openstack-responses')

    s = socket.socket()
    s.connect((sys.argv[6], int(sys.argv[7])))

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
