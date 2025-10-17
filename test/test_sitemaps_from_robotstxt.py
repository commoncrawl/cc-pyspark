from io import BytesIO

import pytest
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from warcio.recordloader import ArcWarcRecord

from sitemaps_from_robotstxt import SitemapExtractorJob
from utils import _process_jobs


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('test_session').getOrCreate()


def make_robots_txt_record(warc_target_uri: str, response_text: str) -> ArcWarcRecord:
    record = MagicMock()
    record.rec_type = 'response'
    record.rec_headers = {'WARC-Target-URI': warc_target_uri}
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
    assert output[0][1] == []


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
    assert sorted(output[0][1]) == sorted(["the-mayflower-hotel-autograph-collection-washington.ibooked.com.br","the-rockies-condominiums-steamboat-springs.booked.net","hotel-flora-venice.booked.kr"])
