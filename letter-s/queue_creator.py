import sys

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 5:
        raise Exception(
            'Please provide: client_id, auth_url, user, key, endpoint')

    conn = client.Connection(sys.argv[1],
                             sys.argv[2],
                             sys.argv[3],
                             sys.argv[4],
                             endpoint=sys.argv[5],
                             token='_')
    conn.connect()

    queue_tasks = conn.create_queue('openstack-tasks', 0)
    queue_responses = conn.create_queue('openstack-responses', 0)
