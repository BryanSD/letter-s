import copy
import json
import socket
import sys
import time

import eventlet
from eventlet import GreenPool
from marconiclient import client


def work_on_it(operation, operand):
    if (operation == 'prime'):
        return work_on_prime(operand)
    elif (operation == 'fibonacci'):
        return work_on_fibonacci(operand)


def work_on_prime(number):
    last_prime = 2

    for num in list(xrange(number + 1))[2:]:
        found_prime = True
        for previous_nums in list(xrange(num))[2:]:
            if (num % previous_nums == 0):
                found_prime = False
                break

        if (found_prime):
            last_prime = num

    print 'Greatest prime under %d: %d' % (number, last_prime)

    return last_prime


def work_on_fibonacci(number):
    n_2 = 0
    n_1 = 1

    for num in list(xrange(number + 1))[2:]:
        n_2, n_1 = n_1, n_2 + n_1

    print 'Fibonacci number %d: %d' % (number, n_1)

    return n_1


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

    pool = GreenPool()

    ttl = 60

    def get_worker_information():
        global ttl

        queue = conn.get_queue('openstack-worker-controller')

        while True:
            messages = list(queue.get_messages(restart=True))

            if len(messages) > 0:
                last_message = messages[-1]
                ttl = last_message['body']['ttl']
            else:
                ttl = 60

            eventlet.sleep(1)

    pool.spawn_n(get_worker_information)

    queue_tasks = conn.get_queue('openstack-tasks')
    queue_results = conn.get_queue('openstack-responses')

    s = socket.socket()
    s.connect((sys.argv[2], int(sys.argv[3])))

    work_item_count = 0
    while True:
        claim = queue_tasks.claim(ttl=60, grace=60)

        for msg in claim.messages:
            #print claim['ttl']
            msg_body = msg['body']

            result_msg = copy.copy(msg_body)

            if msg_body['job_type'] == 'fibonacci':
                result_msg['result'] = str(work_on_fibonacci(msg_body['start_value']))
            elif msg_body['job_type'] == 'prime':
                result_msg['result'] = str(work_on_prime(msg_body['start_value']))

            msg.delete()
            queue_results.post_message(result_msg, ttl)
            work_item_count += 1

            graphite_message = 'openstack.workers.result.sum %d %d\n' % (
                work_item_count, int(time.time()))
            s.sendall(graphite_message)

    s.close()

    pool.waitall()
