import socket
import sys
import time

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


def work_on_fibonacci(number):
    n_2 = 0
    n_1 = 1

    for num in list(xrange(number + 1))[2:]:
        n_2, n_1 = n_1, n_2 + n_1

    print 'Fibonacci number %d: %d' % (number, n_1)


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

    work_item_count = 0
    while True:
        work_items = queue.claim_messages()

        # TODO(bryansd): the work

        graphite_message = 'openstack.workers.result.sum %d %d\n' % (
            work_item_count, int(time.time()))
        s.sendall(graphite_message)

    s.close()
