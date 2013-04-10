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

            print messages_adjusted

            graphite_message = 'openstack.queue.work.rate %d %d\n' % (
                messages_adjusted, int(time_marker))
            s.sendall(graphite_message)

            start_time = time_marker
            messages_created = messages_created - messages_adjusted

    s.close()
