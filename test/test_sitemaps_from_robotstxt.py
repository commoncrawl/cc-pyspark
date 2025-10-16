from io import BytesIO

import pytest
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from warcio.recordloader import ArcWarcRecord

from sitemaps_from_robotstxt import SitemapExtractorJob

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('test_session').getOrCreate()


def make_robots_txt_record(warc_target_uri: str, response_text: str) -> ArcWarcRecord:
    record = MagicMock()
    record.rec_type = 'response'
    d = {'WARC-Target-URI': warc_target_uri}
    record.__getitem__.side_effect = d.__getitem__
    record.content_stream = lambda: BytesIO(response_text.encode('utf-8'))
    return record


def test_well_formed_record(spark):
    record = make_robots_txt_record("http://ajedrezhoygol.blogspot.com.ar/robots.txt",
                                    """User-agent: Mediapartners-Google
Disallow: 

User-agent: *
Disallow: /search
Allow: /

Sitemap: http://ajedrezhoygol.blogspot.com/sitemap.xml
""")
    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    assert len(results) == 1
    assert results[0][0] == 'http://ajedrezhoygol.blogspot.com/sitemap.xml'
    assert results[0][1] == ["ajedrezhoygol.blogspot.com.ar"]


def test_empty_record(spark):
    record = make_robots_txt_record("http://agencasinosbobet5.weebly.com/robots.txt",
"""Sitemap: http://agencasinosbobet5.weebly.com/sitemap.xml

User-agent: NerdyBot
Disallow: /

User-agent: *
Disallow: /ajax/
Disallow: /apps/
""")
    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    assert len(results) == 1
    assert results[0][0] == 'http://agencasinosbobet5.weebly.com/sitemap.xml'
    assert results[0][1] == []


