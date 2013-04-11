import sys

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception('Please provide: Marconi endpoint')

    conn = client.Connection('0',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=sys.argv[1])
    conn.connect('_')

    conn.create_queue('openstack-tasks')
    conn.create_queue('openstack-responses')
    conn.create_queue('openstack-producer-controller')
    conn.create_queue('openstack-worker-controller')
