from fastwarc.warc import WarcRecordType

from sparkcc_fastwarc import CCFastWarcSparkJob
from sitemaps_from_robotstxt import SitemapExtractorJob


class SitemapExtractorFastWarcJob(SitemapExtractorJob, CCFastWarcSparkJob):
    """Extract sitemap URLs (http://www.sitemaps.org/) from robots.txt WARC files
       using FastWARC to parse WARC files."""

    name = "SitemapExtractorFastWarc"

    # process only WARC response and metadata (including WAT) records
    fastwarc_record_filter = WarcRecordType.response

    # process_record is implemented by SitemapExtractorJob
