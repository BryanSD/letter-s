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
    conn.connect('_')

    queue_tasks = conn.get_queue('openstack-tasks')
    queue_responses = conn.get_queue('openstack-responses')

    s = socket.socket()
    s.connect((sys.argv[6], int(sys.argv[7])))

    start_time = time.time()
    messages_created = 0

    while True:
        tasks = queue_tasks.stats()['messages']['total']
        tasks_expired = queue_tasks.stats()['messages']['expired']
        print tasks - tasks_expired
        graphite_message = 'openstack.queue.work.count %d %d\n' % (
            tasks - tasks_expired, int(time.time()))
        s.sendall(graphite_message)

        responses = queue_responses.stats()['messages']['total']
        responses_expired = queue_responses.stats()['messages']['expired']
        print responses - responses_expired
        graphite_message = 'openstack.queue.result.count %d %d\n' % (
            responses - responses_expired, int(time.time()))
        s.sendall(graphite_message)

    s.close()
