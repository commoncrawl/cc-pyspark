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


