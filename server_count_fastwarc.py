import ujson as json

from fastwarc.warc import WarcRecordType

from sparkcc_fastwarc import CCFastWarcSparkJob
from server_count import ServerCountJob


class ServerCountFastWarcJob(CCFastWarcSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input) using FastWARC
        to parse WARC files"""

    name = "CountServers"

    # process only WARC response and metadata (including WAT) records
    fastwarc_record_filter = WarcRecordType.metadata | WarcRecordType.response

    def process_record(self, record):
        # Notes:
        # - HTTP headers may include multiple "Server" headers, often indicating
        #   a delivery cascade, e.g. nginx + idlb1 (load balancer).
        #   We iterate over all "Server" headers in WARC records, in order to count
        #   multiple (but unique) "Server" headers.
        # - WAT records store HTTP headers as JSON objects not preserving multiple
        #   headers, see https://github.com/commoncrawl/ia-web-commons/issues/18

        if (self.is_wat_json_record(record)):
            # WAT (response) record
            record = json.loads(record.reader.read())
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
                                       record.headers['WARC-Target-URI'])

        elif record.record_type == WarcRecordType.response:
            # WARC response record
            server_names = set()
            for (name, value) in record.http_headers.astuples():
                if name.lower() == 'server':
                    if value == '':
                        pass
                    elif value in server_names:
                        self.get_logger().debug(
                            "Not counting duplicated 'Server' header value for %s: %s",
                            record.headers['WARC-Target-URI'], value)
                    else:
                        yield value, 1
                        server_names.add(value)
            if not server_names:
                yield ServerCountJob.fallback_server_name, 1
            elif len(server_names) > 1:
                self.get_logger().info(
                    "Multiple 'Server' header values for %s: %s",
                    record.headers['WARC-Target-URI'], server_names)

        else:
            # warcinfo, request, non-WAT metadata records
            pass


if __name__ == "__main__":
    job = ServerCountFastWarcJob()
    job.run()
