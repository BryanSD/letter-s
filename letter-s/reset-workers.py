#!/usr/bin/env python

import requests
import json
import optparse

def main():
    parser = optparse.OptionParser()
    parser.add_option('--ip', '-I', default = '166.78.143.130')
    options, arguments = parser.parse_args()

    command_msg_ttl = 60
    worker_msg_ttl = 60
    work_item_count = 0
    ip_address = options.ip

    url = "http://{0}/v1/1/queues/openstack-worker-controller/messages".format(ip_address)

    headers = {'Client-Id': 'worker'}
    payload = [
        {
            'ttl': command_msg_ttl,
            'body': {'ttl': worker_msg_ttl,
                     'work_item_count': work_item_count,
                     }
            }
        ]

    print "\nResetting the worker item count to zero\n"

    print "Sending instructions to worker control queue at {0}\n".format(
        options.ip
        )

    r = requests.post(url, headers=headers, data=json.dumps(payload))
    print "RESPONSE FROM CONTROL QUEUE:"
    print r.status_code
    print r.headers

if __name__ == '__main__':
    main()


