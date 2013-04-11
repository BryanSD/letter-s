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

    conn.create_queue('openstack-tasks', 0)
    conn.create_queue('openstack-responses', 0)
    conn.create_queue('openstack-producer-controller', 0)
    conn.create_queue('openstack-worker-controller', 0)
