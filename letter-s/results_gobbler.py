import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 6:
        raise Exception(
            """Please provide: client_id, auth_url, user,
            key, endpoint""")

    conn = client.Connection(sys.argv[1],
                             sys.argv[2],
                             sys.argv[3],
                             sys.argv[4],
                             endpoint=sys.argv[5],
                             token='_')
    conn.connect()

    queue = conn.get_queue('openstack-responses')

    while True:
        messages = list(queue.get_messages())
        message_count = len(messages)

        if message_count == 0:
            print 'Gobbled 0 messages'
            time.sleep(1)
            continue

        for message in messages:
            print message.href
            message.delete()

        print 'Gobbled %d messages' % message_count
