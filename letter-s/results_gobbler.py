import argparse

from marconiclient import client


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('endpoint', help="Marconi endpoint")
    parser.add_argument('-c', '--continuous', action='store_true',
                        help='Continuously drain messages')
    args = parser.parse_args()

    conn = client.Connection('3',
                             'http://example.com',
                             'marconi-demo',
                             'password',
                             endpoint=args.endpoint)
    conn.connect('_')

    queue = conn.get_queue('openstack-responses')

    first_run = True
    while first_run or args.continuous:
        first_run = False

        messages = list(queue.get_messages())
        message_count = len(messages)
        for message in messages:
            message.delete()

        print 'Gobbled %d messages' % message_count
