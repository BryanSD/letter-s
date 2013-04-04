import socket
import sys
import time

import requests


_endpoint = 'http://166.78.143.130:80'
_uuid = '3d3d67f0-9bd5-11e2-9e96-0800200c9a66'


if __name__ == "__main__":
    uri = _endpoint + '/v1/1/queues/%s/messages?echo=true&limit=100000&marker=' % sys.argv[1]
    headers = {'Client-ID': _uuid}

    s = socket.socket()
    s.connect(('166.78.61.156', 2023))

    start_time = time.time()
    messages_created = 0
    while True:
        response = requests.get(uri, headers=headers)

        try:
            msg_json = response.json()
        except ValueError:
            continue

        messages_created += len(msg_json['messages'])

        uri = _endpoint + msg_json['links'][0]['href']

        time_marker = time.time()
        time_diff = time_marker - start_time
        if (time_diff >= 1):
            # Adjust for times greater than 1 second
            messages_adjusted = (1 / time_diff) * messages_created

            #print messages_adjusted
            graphite_message = 'openstack.queue.work.rate %d %d\n' % (
                messages_adjusted, int(time_marker))
            s.sendall(graphite_message)

            start_time = time_marker
            messages_created = messages_created - messages_adjusted

        #time.sleep(1)

    s.close()

