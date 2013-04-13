import copy
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

    # print 'Greatest prime under %d: %d' % (number, last_prime)

    return last_prime


def work_on_fibonacci(number):
    n_2 = 0
    n_1 = 1

    for num in list(xrange(number + 1))[2:]:
        n_2, n_1 = n_1, n_2 + n_1

    # print 'Fibonacci number %d: %d' % (number, n_1)

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
    work_item_count = 0

    def get_worker_information():
        global ttl, work_item_count
        queue = conn.get_queue('openstack-worker-controller')

        while True:
            try:
                messages = list(queue.get_messages())

                if len(messages) > 0:
                    last_message = messages[-1]
                    ttl = last_message['body']['ttl']
                    reset_count = last_message['body']['work_item_count']
                    if reset_count >= 0:
                        work_item_count = reset_count
            except Exception as ex:
                print ex

            eventlet.sleep(1)

    pool.spawn_n(get_worker_information)

    def post_stats():
        global work_item_count

        s = socket.socket()
        s.connect((sys.argv[2], int(sys.argv[3])))

        while True:
            start_time = time.time()

            graphite_message = 'openstack.worker.result.sum %d %d\n' % (
                work_item_count, int(time.time()))
            s.sendall(graphite_message)

            eventlet.sleep(1 - (time.time() - start_time))

        s.close()

    pool.spawn_n(post_stats)

    def post_result_async(original_message, result_message):
        global queue_results, ttl, work_item_count

        original_message.delete()
        queue_results.post_message(result_message, ttl)
        work_item_count += 1

    queue_tasks = conn.get_queue('openstack-tasks')
    queue_results = conn.get_queue('openstack-responses')

    while True:
        try:
            claim = queue_tasks.claim(ttl=60, grace=60)

            if len(claim.messages) == 0:
                time.sleep(1)
                continue

            for msg in claim.messages:
                msg_body = msg['body']

                result_msg = copy.copy(msg_body)

                result_msg['result'] = str(work_on_it(msg_body['job_type'],
                                                      msg_body['start_value']))

                pool.spawn_n(post_result_async, msg, result_msg)
        except Exception as ex:
            print ex

    pool.waitall()
