#!/usr/bin/env python

import marconiclient
import optparse
import uuid

def main():
    parser = optparse.OptionParser()
    parser.add_option('--host', '-H', default = '166.78.143.130')
    parser.add_option('--tenant', '-t', default = '6789')
    options, arguments = parser.parse_args()

    client_id = uuid.uuid1().hex
    url = 'http://{0}/v1/{1}'.format(options.host, options.tenant)

    conn = marconiclient.client.Connection(client_id,
                                           'http://not-authenticating.com',
                                           'not-authenticating',
                                           'no-password',
                                           endpoint=url)

    conn.connect('not-authenticating-so-this-really-isnt-a-token')

    queues = conn.get_queues()
    for queue in queues:
        print queue.name

if __name__ == '__main__':
    main()


