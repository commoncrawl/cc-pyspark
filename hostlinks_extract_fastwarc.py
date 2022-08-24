import ujson as json

from fastwarc.warc import WarcRecordType

from wat_extract_links import ExtractHostLinksJob, ExtractLinksJob
from sparkcc_fastwarc import CCFastWarcSparkJob

class ExtractHostLinksFastWarcJob(CCFastWarcSparkJob, ExtractHostLinksJob):
    """Extract links from WAT files, redirects from WARC files,
    and sitemap links from robots.txt response records relying on the
    FastWARC parser.
    Extract the host names, reverse the names (example.com -> com.example)
    and save the pairs <from_host, to_host>."""

    # process only WARC response and metadata (including WAT) records
    fastwarc_record_filter = WarcRecordType.metadata | WarcRecordType.response

    def iterate_records(self, warc_uri, archive_iterator):
        """Iterate over all WARC records and process them"""
        self.processing_robotstxt_warc \
            = ExtractLinksJob.robotstxt_warc_path_pattern.match(warc_uri)
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res
            self.records_processed.add(1)

    def process_record(self, record):
        link_count = 0
        if self.is_wat_json_record(record):
            try:
                wat_record = json.loads(record.reader.read())
            except ValueError as e:
                self.get_logger().error('Failed to load JSON: {}'.format(e))
                self.records_failed.add(1)
                return
            warc_header = wat_record['Envelope']['WARC-Header-Metadata']
            if warc_header['WARC-Type'] != 'response':
                # WAT request or metadata records
                return
            self.records_response.add(1)
            self.records_response_wat.add(1)
            url = warc_header['WARC-Target-URI']
            for link in self.get_links(url, wat_record):
                link_count += 1
                yield link
        elif record.record_type == WarcRecordType.response:
            self.records_response.add(1)
            self.records_response_warc.add(1)
            stream = record.reader
            http_status_line = stream.readline()
            if (self.processing_robotstxt_warc and ExtractLinksJob
                    .http_success_pattern.match(http_status_line)):
                self.records_response_robotstxt.add(1)
                for link in self.process_robotstxt(record, stream,
                                                   http_status_line):
                    link_count += 1
                    yield link
            elif ExtractLinksJob.http_redirect_pattern.match(http_status_line):
                self.records_response_redirect.add(1)
                for link in self.process_redirect(record, stream,
                                                  http_status_line):
                    link_count += 1
                    yield link
        else:
            return
        if link_count == 0:
            # ensure that the URL itself is a node in the graph
            # (every visited URL should be a node)
            uri = record.headers['WARC-Target-URI']
            for link in self.yield_link(uri, uri):
                link_count += 1
                yield link
        self.link_count.add(link_count)

    def process_redirect(self, record, stream, http_status_line):
        """Process redirects (HTTP status code 30[12378])
        and yield redirect links"""
        line = stream.readline()
        while line:
            m = ExtractLinksJob.http_redirect_location_pattern.match(line)
            if m:
                redir_to = m.group(1).strip()
                try:
                    redir_to = redir_to.decode('utf-8')
                except UnicodeError as e:
                    self.get_logger().warning(
                        'URL with unknown encoding: {} - {}'.format(
                            redir_to, e))
                    return
                redir_from = record.headers['WARC-Target-URI']
                for link in self.yield_link(redir_from, redir_to):
                    yield link
                return
            elif line == b'\r\n':
                # end of HTTP header
                return
            line = stream.readline()

    def process_robotstxt(self, record, stream, _http_status_line):
        """Process robots.txt and yield sitemap links"""
        line = stream.readline()
        while line:
            if line == b'\r\n':
                # end of HTTP header
                break
            line = stream.readline()
        line = stream.readline(crlf=False)
        while line:
            m = ExtractLinksJob.robotstxt_sitemap_pattern.match(line)
            if m:
                sitemap = m.group(1).strip()
                try:
                    sitemap = sitemap.decode('utf-8')
                    from_robotstxt = record.headers['WARC-Target-URI']
                    src_host = ExtractHostLinksJob.get_surt_host(from_robotstxt)
                    thost = ExtractHostLinksJob.get_surt_host(sitemap)
                    if thost and src_host and src_host != thost:
                        yield src_host, thost
                except UnicodeError as e:
                    self.get_logger().warning(
                        'URL with unknown encoding: {} - {}'.format(
                            sitemap, e))
            line = stream.readline(crlf=False)


if __name__ == "__main__":
    job = ExtractHostLinksFastWarcJob()
    job.run()
