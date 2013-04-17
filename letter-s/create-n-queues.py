#!/usr/bin/env python

import marconiclient
import optparse
import uuid
import eventlet

def main():
    parser = optparse.OptionParser()
    parser.add_option('--num', '-n', default = 1)
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
    
    evt_pool = eventlet.GreenPool()

    print "Waiting for create jobs to finish..."
    for i in range (0,int(options.num)):
        queue_name = 'bulk-created-{0:07d}'.format(i)
        evt_pool.spawn_n(conn.create_queue,queue_name)

    evt_pool.waitall()

if __name__ == '__main__':
    main()


