import ujson as json
import re

from sparkcc import CCSparkJob


class ServerCountJob(CCSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input)"""

    name = "CountServers"
    server_http_header_pattern = re.compile('^server:\s*(.+)$', re.IGNORECASE)
    fallback_server_name = '(no server in HTTP header)'

    def process_record(self, record):
        if (record['WARC-Type'] == 'metadata' and
                record['Content-Type'] == 'application/json'):
            # WAT response record
            record = json.loads(record.payload.read())
            try:
                yield record['Envelope'] \
                            ['Payload-Metadata'] \
                            ['HTTP-Response-Metadata'] \
                            ['Headers'] \
                            ['Server'].strip(), 1
            except KeyError:
                yield '(no server in HTTP header)', 1
        elif record['WARC-Type'] == 'response':
            # WARC response record
            for line in record.payload:
                match = ServerCountJob.server_http_header_pattern.match(line)
                if match:
                    yield match.group(1).strip(), 1
                    return
                elif line.strip() == '':
                    # empty line indicates end of HTTP response header
                    yield ServerCountJob.fallback_server_name, 1
                    return


if __name__ == "__main__":
    job = ServerCountJob()
    job.run()
