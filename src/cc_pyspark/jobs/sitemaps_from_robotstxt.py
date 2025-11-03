import re
from urllib.parse import urlparse, urljoin

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from warcio.recordloader import ArcWarcRecord

from cc_pyspark.sparkcc import CCSparkJob

class SitemapExtractorJob(CCSparkJob):
    """Extract sitemap URLs (http://www.sitemaps.org/) from robots.txt WARC files."""

    name = "SitemapExtractor"

    output_schema = StructType([
        StructField('sitemap_url', StringType(), True),
        StructField('hosts', ArrayType(elementType=StringType()), True)
    ])

    # rb: match on raw bytes so we can defer utf-8 decoding to the `sitemap:` line
    sitemap_pattern = re.compile(rb'^sitemap:\s*(\S+)', re.I)

    robots_txt_processed = None
    sitemap_urls_found = None
    sitemap_url_invalid_encoding = None
    robots_txt_announcing_sitemap = None
    robots_txt_with_more_than_50_sitemaps = None


    def log_accumulators(self, session):
        super(SitemapExtractorJob, self).log_accumulators(session)

        self.log_accumulator(session, self.robots_txt_processed,
                             'robots.txt successfully parsed = {}')
        self.log_accumulator(session, self.sitemap_urls_found,
                             'sitemap urls found = {}')
        self.log_accumulator(session, self.sitemap_url_invalid_encoding,
                             'sitemap urls with invalid utf-8 encoding = {}')
        self.log_accumulator(session, self.robots_txt_announcing_sitemap,
                             'robots.txt announcing at least 1 sitemap = {}')
        self.log_accumulator(session, self.robots_txt_with_more_than_50_sitemaps,
                             'robots.txt with more than 50 sitemaps = {}')


    def init_accumulators(self, session):
        super(SitemapExtractorJob, self).init_accumulators(session)

        sc = session.sparkContext
        self.robots_txt_processed = sc.accumulator(0)
        self.sitemap_urls_found = sc.accumulator(0)
        self.sitemap_url_invalid_encoding = sc.accumulator(0)
        self.robots_txt_announcing_sitemap = sc.accumulator(0)
        self.robots_txt_with_more_than_50_sitemaps = sc.accumulator(0)


    def process_record(self, record: ArcWarcRecord):
        """ emit: sitemap_url => [host] """
        if not self.is_response_record(record):
            # we're only interested in the HTTP responses
            return

        self.robots_txt_processed.add(1)
        # robots_txt url/host are lazily computed when we encounter the first valid sitemap URL
        robots_txt_url = None
        robots_txt_host = None
        n_sitemaps = 0

        data = self.get_payload_stream(record).read()
        for raw_line in data.splitlines():
            raw_line = raw_line.strip()

            match = SitemapExtractorJob.sitemap_pattern.match(raw_line)
            if match:
                sitemap_url = match.group(1).strip()
                self.sitemap_urls_found.add(1)
                try:
                    sitemap_url = sitemap_url.decode("utf-8", "strict")
                except UnicodeDecodeError as e:
                    self.get_logger().warn(f'Invalid encoding of sitemap URL {sitemap_url}: {repr(e)}')
                    self.sitemap_url_invalid_encoding.add(1)
                    continue

                if robots_txt_url is None:
                    # first sitemap found: set base URL and get host from URL
                    robots_txt_url = self.get_warc_header(record, 'WARC-Target-URI')
                    try:
                        robots_txt_host = urlparse(robots_txt_url).netloc.lower().lstrip('.')
                    except Exception as e1:
                        try:
                            self.get_logger().warn(f'Invalid robots.txt URL: {robots_txt_url} - {repr(e1)}')
                        except Exception as e2:
                            self.get_logger().warn(f'Invalid robots.txt URL - {repr(e1)} (cannot display: {repr(e2)})')
                        # skip this entire robots.txt record
                        return

                if not (sitemap_url.startswith('http:') or sitemap_url.startswith('https:')):
                    # sitemap_url is relative; pass straight to urljoin which knows how to handle it correctly
                    try:
                        sitemap_url = urljoin(robots_txt_url, sitemap_url)
                    except Exception as e:
                        try:
                            self.get_logger().warn(f'Error joining sitemap URL {sitemap_url} with base {robots_txt_url}: {repr(e)}')
                        except Exception as log_e:
                            self.get_logger().warn(f'Error joining sitemap URL with base - {repr(e)} (cannot display: {repr(log_e)})')
                        continue

                yield sitemap_url, [robots_txt_host]
                n_sitemaps += 1

        if n_sitemaps > 0:
            self.robots_txt_announcing_sitemap.add(1)
            if n_sitemaps > 50:
                self.robots_txt_with_more_than_50_sitemaps.add(1)


    def _try_parse_host(self, url: str, label_for_log: str) -> str|None:
        try:
            return urlparse(url).netloc.lower().lstrip('.')
        except Exception as e:
            try:
                self.get_logger().warn(f'Invalid {label_for_log} URL: {url} - {repr(e)}')
            except Exception as log_e:
                self.get_logger().warn(f'Invalid {label_for_log} URL - {repr(e)} (cannot display: {repr(log_e)})')
            return None


if __name__ == '__main__':
    job = SitemapExtractorJob()
    job.run()
