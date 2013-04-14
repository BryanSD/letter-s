#!/usr/bin/env python

import requests
import json
import optparse

def main():
    parser = optparse.OptionParser()
    parser.add_option('--rate', '-r', default = 0)
    parser.add_option('--ttl', '-t', default = 60)
    parser.add_option('--ip', '-I', default = '166.78.143.130')
    options, arguments = parser.parse_args()

    command_msg_ttl = 60
    worker_msg_ttl = options.ttl
    producer_msg_rate = options.rate
    ip_address = options.ip

    url = "http://{0}/v1/1/queues/openstack-producer-controller/messages".format(ip_address)

    headers = {'Client-Id': 'producer'}
    payload = [
        {
            'ttl': command_msg_ttl,
            'body': {'ttl': worker_msg_ttl,
                     'rate': producer_msg_rate}
            }
        ]

    print "\nSetting producer generation rate to {0} msgs/sec with a TTL of {1} seconds\n".format(
        producer_msg_rate,
        worker_msg_ttl
        )

    print "Sending instructions to producer control queue at {0}\n".format(
        ip_address
        )

    r = requests.post(url, headers=headers, data=json.dumps(payload))
    print "RESPONSE FROM CONTROL QUEUE:"
    print r.status_code
    print r.headers

if __name__ == '__main__':
    main()


