import socket
import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception(
            'Please provide: Marconi endpoint, Graphite IP, Graphite Port')

    conn = client.Connection('0',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=sys.argv[1])
    conn.connect('_')

    queue_tasks = conn.get_queue('openstack-tasks')
    queue_responses = conn.get_queue('openstack-responses')

    s = socket.socket()
    s.connect((sys.argv[2], int(sys.argv[3])))

    while True:
        try:
            task_message_stats = queue_tasks.get_stats().messages
            tasks = task_message_stats['total']
            tasks_expired = task_message_stats['expired']
            graphite_message = 'openstack.queue.work.count %d %d\n' % (
                tasks - tasks_expired, int(time.time()))
            s.sendall(graphite_message)

            response_message_stats = queue_responses.get_stats().messages
            responses = response_message_stats['total']
            responses_expired = response_message_stats['expired']
            graphite_message = 'openstack.queue.result.count %d %d\n' % (
                responses - responses_expired, int(time.time()))
            s.sendall(graphite_message)
        except Exception as ex:
            print ex

    s.close()
