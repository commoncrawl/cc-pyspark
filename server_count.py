import ujson as json

from sparkcc import CCSparkJob


class ServerCountJob(CCSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input)"""

    name = "CountServers"
    fallback_server_name = '(no server in HTTP header)'

    def process_record(self, record):
        # Notes:
        # - HTTP headers may include multiple "Server" headers, often indicating
        #   a delivery cascade, e.g. nginx + idlb1 (load balancer).
        #   We iterate over all "Server" headers in WARC records, in order to count
        #   multiple (but unique) "Server" headers.
        # - WAT records store HTTP headers as JSON objects not preserving multiple
        #   headers, see https://github.com/commoncrawl/ia-web-commons/issues/18

        if self.is_wat_json_record(record):
            # WAT (response) record
            record = json.loads(record.content_stream().read())
            try:
                payload = record['Envelope']['Payload-Metadata']
                if 'HTTP-Response-Metadata' in payload:
                    try:
                        server_name = payload['HTTP-Response-Metadata'] \
                                             ['Headers'] \
                                             ['Server'] \
                                             .strip()
                        if server_name and server_name != '':
                            yield server_name, 1
                        else:
                            yield ServerCountJob.fallback_server_name, 1
                    except KeyError:
                        yield ServerCountJob.fallback_server_name, 1
                else:
                    # WAT request or metadata records
                    pass
            except KeyError:
                self.get_logger().warn("No payload metadata in WAT record for %s",
                                       record.rec_headers.get_header('WARC-Target-URI'))

        elif record.rec_type == 'response':
            # WARC response record
            server_names = set()
            for (name, value) in record.http_headers.headers:
                if name.lower() == 'server':
                    if value == '':
                        pass
                    elif value in server_names:
                        self.get_logger().debug(
                            "Not counting duplicated 'Server' header value for %s: %s",
                            record.rec_headers.get_header('WARC-Target-URI'), value)
                    else:
                        yield value, 1
                        server_names.add(value)
            if not server_names:
                yield ServerCountJob.fallback_server_name, 1
            elif len(server_names) > 1:
                self.get_logger().info(
                    "Multiple 'Server' header values for %s: %s",
                    record.rec_headers.get_header('WARC-Target-URI'), server_names)

        else:
            # warcinfo, request, non-WAT metadata records
            pass


if __name__ == "__main__":
    job = ServerCountJob()
    job.run()
