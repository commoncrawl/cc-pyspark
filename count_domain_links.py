"""Count weighted domain-level links between a fixed list of domains.

Fetches WARC response records via the CC columnar index for pages belonging
to domains in --target_domains, parses HTML for <a href> links, and counts
all link occurrences per (source_domain, target_domain) pair where BOTH the
source and target are in the domain list.

Computes a link-weight matrix within a closed domain set (e.g. domains that are
linking to each other).

Output schema: s STRING, t STRING, nav_count LONG, body_count LONG,
                              related_count LONG, other_count LONG
  s, t           -- registered domains (eTLD+1), e.g. 'bbc.co.uk'
  nav_count      -- links inside <header>, <footer>, <nav>, or elements
                    whose class/id suggests site-wide structural navigation
  body_count     -- links inside <p>, <article>, <main>, or elements whose
                    class/id suggests article body / prose content
  related_count  -- links inside elements whose class/id suggests related-
                    article widgets or recommendation carousels
  other_count    -- everything else: sidebars, bylines, share buttons,
                    uncontextualized links

Usage notes: for sites which rely on <div> elements in combination with CSS to structure their pages, all links might end up in the "other" category.
If the number of target domains is in the millions, the Spark job is likely to fail due to the use of broadcast variables. 
In that case, consider disabling the filtering step by removing the broadcast variable target_domains_bc and returning all outlinks from the target domain list.
In this case the increased output size will require more output partitions when running on AWS EMR (via run_ccpyspark_job_aws_emr.sh).

# TODO: add option to disable the in-memory broadcast variable filtering step and instead filter the output DataFrame after the link extraction step.
# This would allow for larger domain lists at the cost of increased output size and processing time.

Usage:
  bash run_ccpyspark_job_aws_emr.sh \\
    s3://bucket/code/count_domain_links.py \\
    s3://bucket/warehouse \\
    s3://commoncrawl/cc-index/table/cc-main/warc/ \\
    domain_links \\
    --input_base_url s3://commoncrawl/ \\
    --target_domains s3://bucket/target_domains.csv \\
    --crawl CC-MAIN-2024-38 \\
    --num_input_partitions 200 \\
    --num_output_partitions 5
"""

import tldextract

# Use the bundled Public Suffix List; disables network fetching which would
# fail in air-gapped environments such as EMR Serverless.
_tld_extract = tldextract.TLDExtract(suffix_list_urls=())

from collections import Counter
from html.parser import HTMLParser
from urllib.parse import urljoin

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from sparkcc import CCIndexWarcSparkJob, CCSparkJob


def _registered_domain(url_or_domain):
    """Return the registered domain (eTLD+1) for a URL or bare domain, or None."""
    ext = _tld_extract(url_or_domain)
    if not ext.domain or not ext.suffix:
        return None
    return '{}.{}'.format(ext.domain, ext.suffix)


class CountDomainLinksJob(CCIndexWarcSparkJob):
    """Count weighted links between a fixed set of domains.

    Only fetches WARCs for domains in --target_domains. Only counts links
    whose target is also in --target_domains. Self-links (s == t) are dropped.
    """

    name = 'CountDomainLinks'

    output_schema = StructType([
        StructField('s', StringType(), True),
        StructField('t', StringType(), True),
        StructField('nav_count', LongType(), True),
        StructField('body_count', LongType(), True),
        StructField('related_count', LongType(), True),
        StructField('other_count', LongType(), True),
    ])

    warc_parse_http_header = True

    # Broadcast set of target domains (plain registered-domain strings)
    target_domains_bc = None

    # Accumulators
    records_html = None
    records_non_html = None
    records_failed = None
    link_count = None

    # ── HTML parsing ───────────────────────────────────────────────────────────

    # Void elements never have a closing tag; don't push them onto the stack.
    _VOID_ELEMENTS = frozenset([
        'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
        'link', 'meta', 'param', 'source', 'track', 'wbr',
    ])

    # Tags that are unambiguously navigational/structural chrome.
    _NAV_TAGS = frozenset(['header', 'footer', 'nav', 'aside'])

    # Tags that are unambiguously article body.
    # <p> is included: editorial citations are almost always inline prose;
    # nav/footer links are almost never wrapped in <p> tags.
    _BODY_TAGS = frozenset(['article', 'main', 'p'])

    # class/id substrings that suggest site-wide structural navigation.
    _NAV_HINTS = frozenset([
        'nav', 'menu', 'header', 'footer', 'breadcrumb',
        'toolbar', 'banner', 'masthead', 'topbar', 'subnav', 'utility',
    ])

    # class/id substrings that suggest article body / prose content.
    _BODY_HINTS = frozenset([
        'article', 'story', 'post', 'entry', 'content', 'body',
        'prose', 'text', 'detail', 'richtext', 'copy',
    ])

    # class/id substrings that suggest related-article / recommendation widgets.
    # Checked after nav/body so explicit body signals (e.g. 'article-related')
    # are still classified as body via _BODY_HINTS first.
    _RELATED_HINTS = frozenset([
        'related', 'recommended', 'recommendation', 'more-stories', 'more_stories',
        'also-read', 'also_read', 'youmaylike', 'you-may-like', 'you_may_like',
        'trending', 'popular', 'most-read', 'most_read', 'read-more', 'read_more',
        'suggested', 'similar', 'see-also', 'see_also',
    ])

    @classmethod
    def _classify_tag(cls, tag, attrs_d):
        """Return 'nav', 'body', 'related', or None (inherit) for an opening tag."""
        if tag in cls._NAV_TAGS:
            return 'nav'
        if tag in cls._BODY_TAGS:
            return 'body'
        class_id = (attrs_d.get('class', '') + ' ' + attrs_d.get('id', '')).lower()
        if any(h in class_id for h in cls._NAV_HINTS):
            return 'nav'
        if any(h in class_id for h in cls._BODY_HINTS):
            return 'body'
        if any(h in class_id for h in cls._RELATED_HINTS):
            return 'related'
        return None  # no signal; inherit from ancestor

    class _LinkParser(HTMLParser):
        """SAX-style parser that classifies each <a href> into one of four buckets.

        Maintains a stack of (tag, context) pairs where context is 'nav', 'body',
        'related', or None (inherit from parent). Each <a> is classified by the
        nearest explicit ancestor context. Uncontextualized links default to 'other'.
        """

        def __init__(self, classify_tag_fn, void_elements):
            super(CountDomainLinksJob._LinkParser, self).__init__()
            self._classify_tag = classify_tag_fn
            self._void_elements = void_elements
            self.base = None
            self.nav_hrefs = []
            self.body_hrefs = []
            self.related_hrefs = []
            self.other_hrefs = []
            self._stack = []  # list of (tag, context)

        def _current_context(self):
            for _, ctx in reversed(self._stack):
                if ctx is not None:
                    return ctx
            return 'other'  # default: no signal → other

        def handle_starttag(self, tag, attrs):
            attrs_d = dict(attrs)
            if tag == 'base' and self.base is None:
                self.base = attrs_d.get('href')

            if tag == 'a':
                href = attrs_d.get('href')
                if href:
                    ctx = self._current_context()
                    if ctx == 'nav':
                        self.nav_hrefs.append(href)
                    elif ctx == 'body':
                        self.body_hrefs.append(href)
                    elif ctx == 'related':
                        self.related_hrefs.append(href)
                    else:
                        self.other_hrefs.append(href)

            if tag not in self._void_elements:
                ctx = self._classify_tag(tag, attrs_d)
                self._stack.append((tag, ctx))

        def handle_endtag(self, tag):
            # Pop back to the most recent matching open tag, tolerating
            # malformed HTML where tags may be unmatched or misnested.
            for i in range(len(self._stack) - 1, -1, -1):
                if self._stack[i][0] == tag:
                    self._stack = self._stack[:i]
                    break

        def error(self, _message):
            pass

    def _get_link_counts(self, url, html_bytes, src_domain):
        """Parse HTML; return (nav, body, related, other) Counters for in-list targets."""
        target_set = self.target_domains_bc.value
        nav_counts = Counter()
        body_counts = Counter()
        related_counts = Counter()
        other_counts = Counter()
        try:
            parser = self._LinkParser(self._classify_tag, self._VOID_ELEMENTS)
            parser.feed(html_bytes.decode('utf-8', errors='replace'))
            base_url = url
            if parser.base:
                try:
                    base_url = urljoin(url, parser.base)
                except Exception:
                    pass
            for href_list, counts in (
                (parser.nav_hrefs, nav_counts),
                (parser.body_hrefs, body_counts),
                (parser.related_hrefs, related_counts),
                (parser.other_hrefs, other_counts),
            ):
                for href in href_list:
                    try:
                        abs_url = urljoin(base_url, href)
                        tdomain = _registered_domain(abs_url)
                    except Exception:
                        continue
                    if tdomain and tdomain in target_set:
                        counts[tdomain] += 1
        except Exception:
            pass
        return nav_counts, body_counts, related_counts, other_counts

    # ── WARC record processing ─────────────────────────────────────────────────

    def process_record(self, record):
        if not self.is_response_record(record):
            return
        if record.http_headers.get_statuscode() != '200':
            return

        # First try WARC-Identified-Payload-Type, if that fails, fall back to Content-Type header field. 
        # Note, both fields may be missing or incorrect, in which case links from that webpage will not be counted.
        ct = record.rec_headers.get_header('WARC-Identified-Payload-Type')
        if ct is None:
            ct = record.http_headers.get_header('Content-Type', '').lower()
        if 'text/html' not in ct and 'application/xhtml+xml' not in ct:
            self.records_non_html.add(1)
            return

        url = record.rec_headers.get_header('WARC-Target-URI')
        if not url:
            return
        src_domain = _registered_domain(url)
        if not src_domain or src_domain not in self.target_domains_bc.value:
            return
        try:
            html_bytes = self.get_payload_stream(record).read()
        except Exception as e:
            self.get_logger().error('Failed to read {}: {}'.format(url, e))
            self.records_failed.add(1)
            return
        self.records_html.add(1)
        nav_counts, body_counts, related_counts, other_counts = \
            self._get_link_counts(url, html_bytes, src_domain)
        all_targets = set(nav_counts) | set(body_counts) | set(related_counts) | set(other_counts)
        for tdomain in all_targets:
            n = nav_counts.get(tdomain, 0)
            b = body_counts.get(tdomain, 0)
            r = related_counts.get(tdomain, 0)
            o = other_counts.get(tdomain, 0)
            self.link_count.add(n + b + r + o)
            yield src_domain, tdomain, n, b, r, o

    # ── Accumulators ───────────────────────────────────────────────────────────

    def init_accumulators(self, session):
        super(CountDomainLinksJob, self).init_accumulators(session)
        sc = session.sparkContext
        self.records_html = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.records_failed = sc.accumulator(0)
        self.link_count = sc.accumulator(0)

    def log_accumulators(self, session):
        super(CountDomainLinksJob, self).log_accumulators(session)
        self.log_accumulator(session, self.records_html,
                             'HTML records processed = {}')
        self.log_accumulator(session, self.records_non_html,
                             'non-HTML records skipped = {}')
        self.log_accumulator(session, self.records_failed,
                             'records failed = {}')
        self.log_accumulator(session, self.link_count,
                             'total in-list links counted = {}')

    # ── Arguments ──────────────────────────────────────────────────────────────

    def add_arguments(self, parser):
        CCSparkJob.add_arguments(self, parser)

        parser.add_argument(
            '--target_domains', required=True,
            help='Path to CSV of domains (one per line or with a header). '
                 'Values are cleaned with domain.split("/")[0] and deduplicated. '
                 'WARCs are fetched only for these domains; only links between '
                 'domains in this list are counted.')
        parser.add_argument(
            '--crawl', required=True,
            help='CC-MAIN crawl ID to filter the columnar index, '
                 'e.g. CC-MAIN-2024-38.')

    # ── Setup ──────────────────────────────────────────────────────────────────

    def _load_target_domains(self, session):
        """Read, clean, deduplicate, and broadcast --target_domains."""
        raw = session.read.csv(self.args.target_domains, header=False)
        first_col = raw.columns[0]
        domains = set()
        for row in raw.select(first_col).collect():
            val = row[0]
            if not val:
                continue
            # Strip path component and normalise
            val = val.split('/')[0].strip().lower()
            if not val or val == 'website' or val == 'domain':
                continue
            domain = _registered_domain(val)
            if domain:
                domains.add(domain)
        
        # Note: This broadcast variable will limit the maximum size of the domain list.
        # If the number of domains is in the millions the Spark job is likely to fail.
        self.target_domains_bc = session.sparkContext.broadcast(domains)
        self.get_logger().info(
            'Target domains loaded: {}'.format(len(domains)))
        return domains

    def _query_cc_index(self, session):
        """Query the CC columnar index for HTML pages in the target domain set."""
        domains = self.target_domains_bc.value
        domain_df = session.createDataFrame(
            [(d,) for d in domains],
            StructType([StructField('host', StringType(), True)])
        )
        result = (
            session.read.parquet(self.args.input)
            .filter(F.col('crawl') == self.args.crawl)
            .filter(F.col('subset') == 'warc')
            .filter((F.col('content_mime_detected') == 'text/html') | 
                (F.col('content_mime_detected') == 'application/xhtml+xml'))
            .join(F.broadcast(domain_df),
                F.col('url_host_registered_domain') == domain_df['host'], how='inner')
            .select('url', 'warc_filename', 'warc_record_offset', 'warc_record_length')
        )
        if self.args.num_input_partitions > 0:
            result = result.repartition(self.args.num_input_partitions)
        result.persist()
        n = result.count()
        self.get_logger().info('CC index records selected: {}'.format(n))
        return result

    # ── Job orchestration ───────────────────────────────────────────────────────

    def run_job(self, session):
        self._load_target_domains(session)
        index_df = self._query_cc_index(session)

        warc_recs = index_df.select(
            'url', 'warc_filename', 'warc_record_offset', 'warc_record_length'
        ).rdd

        output = warc_recs.mapPartitions(self.fetch_process_warc_records)

        (
            session.createDataFrame(output, schema=self.output_schema)
            .dropna(subset=['s', 't'])
            .groupBy('s', 't')
            .agg(
                F.sum('nav_count').alias('nav_count'),
                F.sum('body_count').alias('body_count'),
                F.sum('related_count').alias('related_count'),
                F.sum('other_count').alias('other_count'),
            )
            .coalesce(self.args.num_output_partitions)
            .write
            .format(self.args.output_format)
            .option('compression', self.args.output_compression)
            .options(**self.get_output_options())
            .saveAsTable(self.args.output)
        )

        self.log_accumulators(session)


if __name__ == '__main__':
    job = CountDomainLinksJob()
    job.run()
