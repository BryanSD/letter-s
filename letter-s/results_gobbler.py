import sys
import time

from marconiclient import client


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Please provide: Marconi endpoint")

    conn = client.Connection('3',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=sys.argv[1])
    conn.connect('_')

    queue = conn.get_queue('openstack-responses')

    while True:
        messages = list(queue.get_messages())
        message_count = len(messages)

        if message_count == 0:
            print 'Gobbled 0 messages'
            continue

        for message in messages:
            # print message.href
            message.delete()

        print 'Gobbled %d messages' % message_count
