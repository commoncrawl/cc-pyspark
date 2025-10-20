import json
from io import BytesIO

import pytest
from unittest.mock import MagicMock, Mock

from pyspark.sql import SparkSession
from warcio.recordloader import ArcWarcRecord

from sitemaps_from_robotstxt import SitemapExtractorJob
from utils import _process_jobs


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('test_session').getOrCreate()


def make_robots_txt_record(warc_target_uri: str, response_text: str,
                           response_text_encoding='utf-8',
                           warc_target_uri_is_invalid=False) -> Mock:
    record = MagicMock()
    record.rec_type = 'response'
    if warc_target_uri_is_invalid:
        # Create an invalid URL that will cause urlparse to fail
        record.rec_headers = {'WARC-Target-URI': warc_target_uri}
    else:
        record.rec_headers = {'WARC-Target-URI': warc_target_uri}
    record.content_stream = lambda: BytesIO(response_text.encode(response_text_encoding))
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



def test_different_host_record(spark):
    record = make_robots_txt_record(
        'http://177.52.3535.ru/robots.txt',
'''
User-agent: Yandex
Disallow: /bitrix/
Disallow: /upload/
Disallow: /detsad/
Disallow: /videouroki/
Disallow: /humor/
Disallow: /radio/
Disallow: /recepts/
Disallow: /school_life/
Disallow: /workgroups/
Disallow: /institutions/
Disallow: /kindergarten/
Disallow: /unified-state-exam/
Disallow: /ideas/
Disallow: /documents/
Disallow: /videosearch/
Disallow: /auth/
Disallow: /demotivators/
Disallow: /additional-education/
Disallow: /admission/
Disallow: /random/
Disallow: /horoscope/
Disallow: /monitoring-in-the-sphere-of-education/
Disallow: /votes/
Disallow: /news/
Disallow: /clips/
Disallow: /preschool-education/
Disallow: /movies/
Disallow: /TV/
Disallow: /dreambook/
Disallow: /about/
Disallow: /company/
Disallow: /edit/
Disallow: /com-docs/
Disallow: /professional-education/
Disallow: /vs.php
Disallow: /index-old.php
Disallow: /404.php
Disallow: /suz/
Disallow: /school-education/
Disallow: /municipal-education-authorities/
Disallow: /com-about/
Disallow: /parents/
Disallow: /view/
Disallow: /stat/
Disallow: /quotes/
Disallow: /region/
Disallow: /students/
Disallow: /graduates/
Disallow: /job/
Disallow: /auth.php
Disallow: /search/
Disallow: /search/
Disallow: /auth/
Disallow: /auth.php
Disallow: /personal/
Disallow: /*?print=
Disallow: /*&print=
Disallow: /*register=yes
Disallow: /*forgot_password=yes
Disallow: /*change_password=yes
Disallow: /*login=yes
Disallow: /*logout=yes
Disallow: /*auth=yes
Disallow: /*action=ADD_TO_COMPARE_LIST
Disallow: /*action=DELETE_FROM_COMPARE_LIST
Disallow: /*action=ADD2BASKET
Disallow: /*action=BUY
Disallow: /*bitrix_*=
Disallow: /*backurl=*
Disallow: /*BACKURL=*
Disallow: /*back_url=*
Disallow: /*BACK_URL=*
Disallow: /*back_url_admin=*
Disallow: /*index.php$
Disallow: /*?*


User-agent: *
Disallow: /bitrix/
Disallow: /upload/
Disallow: /detsad/
Disallow: /humor/
Disallow: /videouroki/
Disallow: /radio/
Disallow: /recepts/
Disallow: /school_life/
Disallow: /workgroups/
Disallow: /institutions/
Disallow: /kindergarten/
Disallow: /unified-state-exam/
Disallow: /ideas/
Disallow: /documents/
Disallow: /videosearch/
Disallow: /auth/
Disallow: /demotivators/
Disallow: /additional-education/
Disallow: /admission/
Disallow: /random/
Disallow: /horoscope/
Disallow: /monitoring-in-the-sphere-of-education/
Disallow: /votes/
Disallow: /news/
Disallow: /clips/
Disallow: /preschool-education/
Disallow: /movies/
Disallow: /TV/
Disallow: /dreambook/
Disallow: /about/
Disallow: /company/
Disallow: /edit/
Disallow: /com-docs/
Disallow: /professional-education/
Disallow: /vs.php
Disallow: /index-old.php
Disallow: /404.php
Disallow: /suz/
Disallow: /school-education/
Disallow: /municipal-education-authorities/
Disallow: /com-about/
Disallow: /parents/
Disallow: /view/
Disallow: /stat/
Disallow: /quotes/
Disallow: /region/
Disallow: /students/
Disallow: /graduates/
Disallow: /job/
Disallow: /auth.php
Disallow: /search/
Disallow: /search/
Disallow: /auth/
Disallow: /auth.php
Disallow: /personal/
Disallow: /*?print=
Disallow: /*&print=
Disallow: /*register=yes
Disallow: /*forgot_password=yes
Disallow: /*change_password=yes
Disallow: /*login=yes
Disallow: /*logout=yes
Disallow: /*auth=yes
Disallow: /*action=ADD_TO_COMPARE_LIST
Disallow: /*action=DELETE_FROM_COMPARE_LIST
Disallow: /*action=ADD2BASKET
Disallow: /*action=BUY
Disallow: /*bitrix_*=
Disallow: /*backurl=*
Disallow: /*BACKURL=*
Disallow: /*back_url=*
Disallow: /*BACK_URL=*
Disallow: /*back_url_admin=*
Disallow: /*index.php$
Disallow: /*?*
Host: 3535.ru


Sitemap: http://3535.ru/sitemap_000.xml
'''
    )
    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    assert len(results) == 1
    assert results[0][0] == 'http://3535.ru/sitemap_000.xml'
    assert results[0][1] == ['177.52.3535.ru']




def test_host_accumulation_empty(spark):
    """
    Test accumulation of hosts when sitemap url host and robots.txt url host match
    Requires test/ on PYTHONPATH so utils._process_jobs can be imported
    """

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

    records = [record]
    rdd = spark.sparkContext.parallelize(records)
    _process_jobs_partial = lambda partition_index, records: _process_jobs(partition_index, records, job=job)
    output = rdd.mapPartitionsWithIndex(_process_jobs_partial)
    output = output.groupByKey().map(SitemapExtractorJob.reduce_group_by_key_func).collect()

    assert len(output) == 1
    assert output[0][0] == 'http://agencasinosbobet5.weebly.com/sitemap.xml'
    assert output[0][1] == '[]'


def test_host_accumulation_multi(spark):
    """
    Test accumulation of multiple hosts for same sitemap URL from different robots.txt records
    Requires test/ on PYTHONPATH so utils._process_jobs can be imported
    """

    multi_robots_txt_data = [
        (
            "http://the-mayflower-hotel-autograph-collection-washington.ibooked.com.br/robots.txt",
            """User-Agent: *
Allow: /
Disallow: /reviewpage/
Disallow: /ajax/
Disallow: /?page=stat
Disallow: /?page=hotel_ajax
Disallow: /?page=hotellist_json
Disallow: /reviewpage2/


User-agent: Yandex
Host: nochi.com
Allow: /
Disallow: /reviewpage/

Disallow: /ajax/
Disallow: /?page=stat
Disallow: /?page=hotel_ajax
Disallow: /?page=hotellist_json
Sitemap: http://nochi.com/data/sitemaps/ru_index.xml
"""
        ),
        (
            "http://the-rockies-condominiums-steamboat-springs.booked.net/robots.txt",
            """User-Agent: *
Allow: /
Disallow: /reviewpage/
Disallow: /ajax/
Disallow: /?page=stat
Disallow: /?page=hotel_ajax
Disallow: /?page=hotellist_json
Disallow: /reviewpage2/


User-agent: Yandex
Host: nochi.com
Allow: /
Disallow: /reviewpage/

Disallow: /ajax/
Disallow: /?page=stat
Disallow: /?page=hotel_ajax
Disallow: /?page=hotellist_json
Sitemap: http://nochi.com/data/sitemaps/ru_index.xml
""",
        ),
        (
            "http://hotel-flora-venice.booked.kr/robots.txt",
            """User-Agent: *
Allow: /
Disallow: /reviewpage/
Disallow: /ajax/
Disallow: /?page=stat
Disallow: /?page=hotel_ajax
Disallow: /?page=hotellist_json
Disallow: /reviewpage2/


User-agent: Yandex
Host: nochi.com
Allow: /
Disallow: /reviewpage/

Disallow: /ajax/
Disallow: /?page=stat
Disallow: /?page=hotel_ajax
Disallow: /?page=hotellist_json
Sitemap: http://nochi.com/data/sitemaps/ru_index.xml
"""
        )
    ]

    records = [make_robots_txt_record(robots_txt_url, robots_txt_content)
                for robots_txt_url, robots_txt_content in multi_robots_txt_data]
    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    rdd = spark.sparkContext.parallelize(records)
    _process_jobs_partial = lambda partition_index, records: _process_jobs(partition_index, records, job=job)
    output = rdd.mapPartitionsWithIndex(_process_jobs_partial)
    output = output.groupByKey().map(SitemapExtractorJob.reduce_group_by_key_func).collect()
    assert len(output) == 1
    assert output[0][0] == 'http://nochi.com/data/sitemaps/ru_index.xml'
    assert sorted(json.loads(output[0][1])) == sorted(["the-mayflower-hotel-autograph-collection-washington.ibooked.com.br","the-rockies-condominiums-steamboat-springs.booked.net","hotel-flora-venice.booked.kr"])


def test_wrong_encoding_utf16_record(spark):
    record = make_robots_txt_record("http://ajedrezhoygol.blogspot.com.ar/robots.txt",
                                    """User-agent: Mediapartners-Google
Disallow: 

User-agent: *
Disallow: /search
Allow: /

Sitemap: http://ajedrezhoygol.blogspot.com/sitemap.xml
""", response_text_encoding='utf-16')
    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    assert len(results) == 0


def test_robots_txt_invalid_url_malformed(spark):
    """Test that robots_txt_invalid_url increments when robots.txt URL causes urlparse to fail"""
    # urlparse will raise AttributeError when given a non-string type in rec_headers
    # We need to mock the record more carefully to trigger an actual exception
    record = MagicMock()
    record.rec_type = 'response'
    # Pass an integer instead of string to cause AttributeError in urlparse
    record.rec_headers = {'WARC-Target-URI': 12345}  # Non-string type
    record.content_stream = lambda: BytesIO(b"""User-agent: *
Disallow: /

Sitemap: http://example.com/sitemap.xml
""")

    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)

    # Mock the logger to avoid serialization issues with Spark's Java logger
    job.get_logger = lambda: MagicMock()

    results = list(job.process_record(record))
    # Should return early due to invalid robots.txt URL
    assert len(results) == 0
    assert job.robots_txt_invalid_url.value == 1


def test_robots_txt_invalid_url_unparseable_netloc(spark):
    """Test that robots_txt_invalid_url increments when robots.txt URL is a list"""
    # Another way to trigger urlparse exception with a non-string type
    record = MagicMock()
    record.rec_type = 'response'
    record.rec_headers = {'WARC-Target-URI': ['http://example.com']}  # List will cause TypeError
    record.content_stream = lambda: BytesIO(b"""User-agent: *
Disallow: /admin/

Sitemap: http://valid-example.com/sitemap.xml
Sitemap: http://valid-example.com/sitemap2.xml
""")

    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)

    # Mock the logger to avoid serialization issues
    job.get_logger = lambda: MagicMock()

    results = list(job.process_record(record))
    # Should return early due to invalid robots.txt URL
    assert len(results) == 0
    assert job.robots_txt_invalid_url.value == 1


def test_robots_txt_invalid_punycode_url(spark):
    """Test handling of invalid punycode domain in robots.txt URL"""
    # xn--foo is an invalid punycode domain (incomplete encoding)
    # validators.url properly rejects it, so this tests that invalid punycode is caught
    record = make_robots_txt_record("http://xn--foo/robots.txt",
                                    """User-agent: *
Disallow: /

Sitemap: http://example.com/sitemap.xml
""")
    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    # validators.url properly detects invalid punycode and rejects it
    # So the robots.txt URL is invalid and no results are returned
    assert len(results) == 0
    assert job.robots_txt_invalid_url.value == 1


def test_sitemap_url_invalid_encoding_latin1(spark):
    """Test that sitemap_url_invalid_encoding increments for non-UTF8 sitemap URLs"""
    # Create a robots.txt with a sitemap URL containing Latin-1 bytes that aren't valid UTF-8
    # The byte sequence \xe9 is é in Latin-1 but invalid in UTF-8 when standalone
    robots_txt_bytes = b"""User-agent: *
Disallow: /

Sitemap: http://example.com/sitemap_caf\xe9.xml
"""
    record = MagicMock()
    record.rec_type = 'response'
    record.rec_headers = {'WARC-Target-URI': 'http://example.com/robots.txt'}
    record.content_stream = lambda: BytesIO(robots_txt_bytes)

    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    assert len(results) == 0
    assert job.sitemap_url_invalid_encoding.value == 1


def test_sitemap_url_invalid_encoding_mixed_bytes(spark):
    """Test that sitemap_url_invalid_encoding increments for mixed invalid byte sequences"""
    # Create a robots.txt with multiple sitemap URLs, one with invalid UTF-8
    # The byte sequence \xff\xfe is not valid UTF-8
    robots_txt_bytes = b"""User-agent: *
Disallow: /search

Sitemap: http://example.com/good_sitemap.xml
Sitemap: http://example.com/bad\xff\xfe_sitemap.xml
Sitemap: http://example.com/another_good.xml
"""
    record = MagicMock()
    record.rec_type = 'response'
    record.rec_headers = {'WARC-Target-URI': 'http://example.com/robots.txt'}
    record.content_stream = lambda: BytesIO(robots_txt_bytes)

    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)
    results = list(job.process_record(record))
    # Should get 2 valid sitemaps and 1 invalid encoding
    assert len(results) == 2
    assert job.sitemap_url_invalid_encoding.value == 1
    assert job.sitemap_urls_found.value == 3  # All 3 matched the pattern


def test_sitemap_url_invalid_malformed_url(spark):
    """Test that sitemap_url_invalid increments when sitemap URL causes validation to fail"""
    robots_txt_bytes = b"""User-agent: *
Disallow: /

Sitemap: http://example.com/sitemap.xml
"""
    record = MagicMock()
    record.rec_type = 'response'
    record.rec_headers = {'WARC-Target-URI': 'http://example.com/robots.txt'}
    record.content_stream = lambda: BytesIO(robots_txt_bytes)

    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)

    # Mock _is_valid_url to return False for sitemap URLs (simulating validation failure)
    original_is_valid = job._is_valid_url
    def mock_is_valid(url, label_for_log):
        if label_for_log == 'sitemap':
            return False  # Simulate validation failure for sitemap URLs
        return original_is_valid(url, label_for_log)

    job._is_valid_url = mock_is_valid

    results = list(job.process_record(record))
    assert len(results) == 0
    assert job.sitemap_url_invalid.value == 1
    assert job.sitemap_urls_found.value == 1


def test_sitemap_url_invalid_unparseable_scheme(spark):
    """Test that sitemap_url_invalid increments for multiple unparseable sitemap URLs"""
    robots_txt_bytes = b"""User-agent: *
Disallow: /admin/

Sitemap: http://valid.com/sitemap1.xml
Sitemap: http://broken.com/sitemap.xml
Sitemap: http://valid.com/sitemap2.xml
"""
    record = MagicMock()
    record.rec_type = 'response'
    record.rec_headers = {'WARC-Target-URI': 'http://example.com/robots.txt'}
    record.content_stream = lambda: BytesIO(robots_txt_bytes)

    job = SitemapExtractorJob()
    job.init_accumulators(session=spark)

    # Mock _is_valid_url to return False for specific sitemap URLs
    original_is_valid = job._is_valid_url
    def mock_is_valid(url, label_for_log):
        if label_for_log == 'sitemap' and 'broken.com' in url:
            return False  # Simulate validation failure for broken.com
        return original_is_valid(url, label_for_log)

    job._is_valid_url = mock_is_valid

    results = list(job.process_record(record))
    # Should get 2 valid sitemaps and 1 invalid
    assert len(results) == 2
    assert job.sitemap_url_invalid.value == 1
    assert job.sitemap_urls_found.value == 3

