import ujson as json

from sparkcc import CCSparkJob


class ServerCountJob(CCSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input)"""

    name = "CountServers"
    fallback_server_name = '(no server in HTTP header)'

    def process_record(self, record):
        server_name = None

        if self.is_wat_json_record(record):
            # WAT (response) record
            record = json.loads(record.content_stream().read())
            try:
                payload = record['Envelope']['Payload-Metadata']
                if 'HTTP-Response-Metadata' in payload:
                    server_name = payload['HTTP-Response-Metadata'] \
                                         ['Headers'] \
                                         ['Server'] \
                                         .strip()
                else:
                    # WAT request or metadata records
                    return
            except KeyError:
                pass
        elif record.rec_type == 'response':
            # WARC response record
            server_name = record.http_headers.get_header('server', None)
        else:
            # warcinfo, request, non-WAT metadata records
            return

        if server_name and server_name != '':
            yield server_name, 1
        else:
            yield ServerCountJob.fallback_server_name, 1


if __name__ == "__main__":
    job = ServerCountJob()
    job.run()
