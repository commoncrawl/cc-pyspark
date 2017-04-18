import ujson as json

from sparkcc import CCSparkJob


class ServerCountJob(CCSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input)"""

    name = "CountServers"
    fallback_server_name = '(no server in HTTP header)'

    def process_record(self, record):
        if (record.rec_type == 'metadata' and
                record.content_type == 'application/json'):
            # WAT response record
            record = json.loads(record.content_stream().read())
            try:
                yield record['Envelope'] \
                            ['Payload-Metadata'] \
                            ['HTTP-Response-Metadata'] \
                            ['Headers'] \
                            ['Server'].strip(), 1
            except KeyError:
                yield ServerCountJob.fallback_server_name, 1
        elif record.rec_type == 'response':
            # WARC response record
            server_name = record.http_headers.get_header(
                'server',
                ServerCountJob.fallback_server_name)
            yield server_name, 1


if __name__ == "__main__":
    job = ServerCountJob()
    job.run()
