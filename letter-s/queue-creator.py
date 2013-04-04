import sys

import requests


_endpoint = 'http://166.78.143.130:8080/'


if __name__ == "__main__":
    uri = _endpoint + 'v1/1/queues/%s' % sys.argv[1]
    response = requests.put(uri, data='{}')

    if (response.status_code not in [201, 204]):
        raise Exception('Queue could not be created.')
