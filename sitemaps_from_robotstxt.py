import re
from typing import Optional
from urllib.parse import urlparse, urljoin, ParseResult

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from warcio.recordloader import ArcWarcRecord

from sparkcc import CCSparkJob

class SitemapExtractorJob(CCSparkJob):
    """Extract sitemap URLs (http://www.sitemaps.org/) from robots.txt WARC files."""

    name = "SitemapExtractor"

    output_schema = StructType([
        StructField('sitemap_url', StringType(), True),
        StructField('hosts', ArrayType(StringType()), True)
    ])

    merge_method = 'reduce_group_by_key'

    # rb: match on raw bytes so we can defer utf-8 decoding to the `sitemap:` line
    sitemap_pattern = re.compile(rb'^sitemap:\s*(\S+)', re.I)

    robots_txt_processed = None
    sitemap_urls_found = None
    sitemap_url_invalid = None
    sitemap_url_invalid_encoding = None
    robots_txt_invalid_url = None
    robots_txt_announcing_sitemap = None
    robots_txt_with_more_than_50_sitemaps = None

    def init_accumulators(self, session):
        super(SitemapExtractorJob, self).init_accumulators(session)

        sc = session.sparkContext
        self.robots_txt_processed = sc.accumulator(0)
        self.sitemap_urls_found = sc.accumulator(0)
        self.sitemap_url_invalid = sc.accumulator(0)
        self.sitemap_url_invalid_encoding = sc.accumulator(0)
        self.robots_txt_invalid_url = sc.accumulator(0)
        self.robots_txt_announcing_sitemap = sc.accumulator(0)
        self.robots_txt_with_more_than_50_sitemaps = sc.accumulator(0)

    @staticmethod
    def reduce_group_by_key_func(kv: tuple):
        """Map sitemap URL to cross-submit hosts:
            sitemap_url => [host_1, ..., host_n]"""
        sitemap_uri, hosts_lists = kv
        try:
            sitemap_host = urlparse(sitemap_uri).netloc.lower().lstrip('.')
        except Exception as e:
            raise RuntimeError("A sitemap URI somehow made it through the initial parsing phase, this shouldn't happen: " + repr(e))

        cross_submit_hosts = set()

        for robots_txt_hosts in hosts_lists:
            for robots_txt_host in robots_txt_hosts:
                if robots_txt_host != sitemap_host:
                    cross_submit_hosts.add(robots_txt_host)

        return sitemap_uri, list(cross_submit_hosts)


    def process_record(self, record: ArcWarcRecord):
        """ emit: sitemap_url => [host] """
        if not self.is_response_record(record):
            # we're only interested in the HTTP responses
            return

        self.robots_txt_processed.add(1)
        url = None
        host = None
        n_sitemaps = 0

        data = self.get_payload_stream(record).read()
        for raw_line in data.splitlines():
            raw_line = raw_line.strip()

            match = SitemapExtractorJob.sitemap_pattern.match(raw_line)
            if match:
                sitemap_url = match.group(1).strip()
                self.sitemap_urls_found.add(1)
                n_sitemaps += 1
                try:
                    sitemap_url = sitemap_url.decode("utf-8", "strict")
                except UnicodeEncodeError:
                    # invalid encoding, ignore
                    self.sitemap_url_invalid_encoding.add(1)
                    continue

                if self._try_parse_url(sitemap_url, label_for_log='sitemap') is None:
                    self.sitemap_url_invalid.add(1)
                    continue

                if url is None:
                    # first sitemap found: set base URL and get host from URL
                    url = record.rec_headers['WARC-Target-URI']
                    url_parsed = self._try_parse_url(url, label_for_log='robots.txt')
                    if url_parsed is None:
                        # skip this robots.txt record
                        self.robots_txt_invalid_url.add(1)
                        return

                    host = url_parsed.netloc.lower().lstrip('.')

                if not sitemap_url.startswith('http'):
                    sitemap_url = urljoin(url, sitemap_url)

                yield sitemap_url, [host]

        if n_sitemaps > 0:
            self.robots_txt_announcing_sitemap.add(1)

        if n_sitemaps > 50:
            self.robots_txt_with_more_than_50_sitemaps.add(1)


    def _try_parse_url(self, url, label_for_log) -> Optional[ParseResult]:
        try:
            return urlparse(url)
        except Exception as url_parse_error:
            try:
                self.get_logger().warn('Invalid %s URL: %s - %s', label_for_log, url, url_parse_error)
            except UnicodeEncodeError as unicode_error:
                self.get_logger().warn('Invalid %s URL (cannot be displayed): %s - %s',
                                       label_for_log, url_parse_error, unicode_error)
            return None


if __name__ == '__main__':
    job = SitemapExtractorJob()
    job.run()
