import sys

import requests


_endpoint = 'http://'


if __name__ == "__main__":
    uri = _endpoint + 'v1/1/queues/%s' % sys.argv[1]
    response = requests.put(uri, data='{}')

    if (response.status_code not in [201, 204]):
        raise Exception('Queue could not be created.')
