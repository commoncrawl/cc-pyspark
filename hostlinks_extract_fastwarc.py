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
