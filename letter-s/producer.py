import socket
import sys
import time

import requests


_endpoint = 'http://'
_uuid = '3d3d67f0-9bd5-11e2-9e96-0800200c9a66'


if __name__ == "__main__":
    uri = _endpoint + 'v1/1/queues/%s/messages' % sys.argv[1]
    headers = {'Client-ID': _uuid}

    s = socket.socket()
    s.connect(('IP_ADDRESS', 0))

    start_time = time.time()
    messages_created = 0
    while True:
        response = requests.post(uri,
                                 headers=headers,
                                 data='[{"ttl": 5, "body": {"test": 1}}]')
        messages_created += 1

        if (response.status_code != 201):
            raise Exception('Message could not be posted.')

        time_marker = time.time()
        time_diff = time_marker - start_time
        if (time_diff >= 1):
            # Adjust for times greater than 1 second
            messages_adjusted = (1 / time_diff) * messages_created

            graphite_message = 'openstack.producer.worker.rate %d %d\n' % (
                messages_adjusted, int(time_marker))
            s.sendall(graphite_message)

            start_time = time.time()
            messages_created = 0

    s.close()
