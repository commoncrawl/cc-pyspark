import re
from urllib.parse import urlparse, urljoin

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from warcio.recordloader import ArcWarcRecord

from sparkcc import CCSparkJob

class SitemapExtractorJob(CCSparkJob):
    """Extract sitemap URLs (http://www.sitemaps.org/) from robots.txt WARC files."""

    name = "SitemapExtractor"

    output_schema = StructType([
        StructField('key', StringType(), True),
        StructField('val', StructType([
            StructField('hosts', ArrayType(StringType()), True)
        ]), True)
    ])

    # rb: match on raw bytes so we can defer utf-8 decoding to the `sitemap:` line
    sitemap_pattern = re.compile(rb'^sitemap:\s*(\S+)', re.I)

    robots_txt_processed = None
    sitemap_urls_found = None
    sitemap_url_invalid_encoding = None
    robots_txt_invalid_url = None
    robots_txt_announcing_sitemap = None
    robots_txt_with_more_than_50_sitemaps = None


    def init_accumulators(self, session):
        super(SitemapExtractorJob, self).init_accumulators(session)

        sc = session.sparkContext
        self.robots_txt_processed = sc.accumulator(0)
        self.sitemap_urls_found = sc.accumulator(0)
        self.sitemap_url_invalid_encoding = sc.accumulator(0)
        self.robots_txt_invalid_url = sc.accumulator(0)
        self.robots_txt_announcing_sitemap = sc.accumulator(0)
        self.robots_txt_with_more_than_50_sitemaps = sc.accumulator(0)


    def process_record(self, record: ArcWarcRecord):
        """ emit: sitemap_url => [host] """
        if not self.is_response_record(record):
            # we're only interested in the HTTP responses
            return

        self.robots_txt_processed.add(1)
        url = None
        host = None
        n_sitemaps = 0

        for raw_line in self.get_payload_stream(record).readlines():
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

                if url is None:
                    # first sitemap found: set base URL and get host from URL
                    url = record['WARC-Target-URI']
                    try:
                        host = urlparse(url).netloc.lower().lstrip('.')
                    except Exception as url_parse_error:
                        try:
                            self.get_logger().warning('Invalid robots.txt URL: %s - %s',
                                                     url, str(url_parse_error))
                        except UnicodeEncodeError as unicode_error:
                            self.get_logger().warning('Invalid robots.txt URL (cannot be displayed) - %s - %',
                                                      str(url_parse_error), str(unicode_error))
                        self.robots_txt_invalid_url.add(1)
                        # skip this robots.txt record
                        return None

                if not sitemap_url.startswith('http'):
                    sitemap_url = urljoin(url, sitemap_url)

                yield sitemap_url, [host]

        if n_sitemaps > 0:
            self.robots_txt_announcing_sitemap.add(1)

        if n_sitemaps > 50:
            self.robots_txt_with_more_than_50_sitemaps.add(1)

if __name__ == '__main__':
    job = SitemapExtractorJob()
    job.run()
