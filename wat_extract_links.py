import idna
import os
import re

import ujson as json

try:
    # Python2
    from urlparse import urljoin, urlparse
except ImportError:
    # Python3
    from urllib.parse import urljoin, urlparse

from pyspark.sql.types import StructType, StructField, StringType

from sparkcc import CCSparkJob


class ExtractLinksJob(CCSparkJob):
    """Extract links from WAT files and redirects from WARC files
    and save them as pairs <from, to>"""
    name = "ExtractLinks"

    output_schema = StructType([
        StructField("s", StringType(), True),
        StructField("t", StringType(), True)
    ])

    warc_parse_http_header = False

    processing_robotstxt_warc = False

    records_response = None
    records_response_wat = None
    records_response_warc = None
    records_response_robotstxt = None
    records_failed = None
    records_non_html = None
    records_response_redirect = None
    link_count = None

    http_redirect_pattern = re.compile(b'^HTTP\\s*/\\s*1\\.[01]\\s*30[12378]\\b')
    http_redirect_location_pattern = re.compile(b'^Location:\\s*(\\S+)',
                                                re.IGNORECASE)
    http_link_pattern = re.compile(r'<([^>]*)>')
    http_success_pattern = re.compile(b'^HTTP\\s*/\\s*1\\.[01]\\s*200\\b')
    robotstxt_warc_path_pattern = re.compile(r'.*/robotstxt/')
    robotstxt_sitemap_pattern = re.compile(b'^Sitemap:\\s*(\\S+)',
                                           re.IGNORECASE)
    url_abs_pattern = re.compile(r'^(?:https?:)?//')

    # Meta properties usually offering links:
    #   <meta property="..." content="https://..." />
    html_meta_property_links = {
        'og:url', 'og:image', 'og:image:secure_url',
        'og:video', 'og:video:url', 'og:video:secure_url',
        'twitter:url', 'twitter:image:src'}
    # Meta names usually offering links
    html_meta_links = {
        'twitter:image', 'thumbnail', 'application-url',
        'msapplication-starturl', 'msapplication-TileImage', 'vb_meta_bburl'}

    def add_arguments(self, parser):
        parser.add_argument("--intermediate_output", type=str,
                            default=None,
                            help="Intermediate output to recover job from")

    @staticmethod
    def _url_join(base, link):
        # TODO: efficiently join without reparsing base
        # TODO: canonicalize
        pass

    def iterate_records(self, warc_uri, archive_iterator):
        """Iterate over all WARC records and process them"""
        self.processing_robotstxt_warc \
            = ExtractLinksJob.robotstxt_warc_path_pattern.match(warc_uri)
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res
            self.records_processed.add(1)

    def process_record(self, record):
        link_count = 0
        if self.is_wat_json_record(record):
            try:
                wat_record = json.loads(record.content_stream().read())
            except ValueError as e:
                self.get_logger().error('Failed to load JSON: {}'.format(e))
                self.records_failed.add(1)
                return
            warc_header = wat_record['Envelope']['WARC-Header-Metadata']
            if warc_header['WARC-Type'] != 'response':
                # WAT request or metadata records
                return
            self.records_response.add(1)
            self.records_response_wat.add(1)
            url = warc_header['WARC-Target-URI']
            for link in self.get_links(url, wat_record):
                link_count += 1
                yield link
        elif record.rec_type == 'response':
            self.records_response.add(1)
            self.records_response_warc.add(1)
            stream = record.content_stream()
            http_status_line = stream.readline()
            if (self.processing_robotstxt_warc and ExtractLinksJob
                    .http_success_pattern.match(http_status_line)):
                self.records_response_robotstxt.add(1)
                for link in self.process_robotstxt(record, stream,
                                                   http_status_line):
                    link_count += 1
                    yield link
            elif ExtractLinksJob.http_redirect_pattern.match(http_status_line):
                self.records_response_redirect.add(1)
                for link in self.process_redirect(record, stream,
                                                  http_status_line):
                    link_count += 1
                    yield link
        else:
            return
        if link_count == 0:
            # ensure that the URL itself is a node in the graph
            # (every visited URL should be a node)
            uri = record.rec_headers.get_header('WARC-Target-URI')
            for link in self.yield_link(uri, uri):
                link_count += 1
                yield link
        self.link_count.add(link_count)

    def process_redirect(self, record, stream, http_status_line):
        """Process redirects (HTTP status code 30[12378])
        and yield redirect links"""
        line = stream.readline()
        while line:
            m = ExtractLinksJob.http_redirect_location_pattern.match(line)
            if m:
                redir_to = m.group(1).strip()
                try:
                    redir_to = redir_to.decode('utf-8')
                except UnicodeError as e:
                    self.get_logger().warning(
                        'URL with unknown encoding: {} - {}'.format(
                            redir_to, e))
                    return
                redir_from = record.rec_headers.get_header('WARC-Target-URI')
                for link in self.yield_link(redir_from, redir_to):
                    yield link
                return
            elif line == b'\r\n':
                # end of HTTP header
                return
            line = stream.readline()

    def process_robotstxt(self, record, stream, http_status_line):
        # Robots.txt -> sitemap links are meaningful for host-level graphs,
        # page-level graphs usually do not contain the robots.txt as a node
        pass

    def yield_redirect(self, src, target, http_status_line):
        if src != target:
            yield src, target

    def yield_http_header_links(self, url, headers):
        if 'Content-Location' in headers:
            yield url, headers['Content-Location']
        if 'Link' in headers:
            for m in ExtractLinksJob.http_link_pattern.finditer(headers['Link']):
                yield url, m.group(1)

    def yield_links(self, src_url, base_url, links, url_attr, opt_attr=None):
        # base_url = urlparse(base)
        if not base_url:
            base_url = src_url
        has_links = False
        for l in links:
            link = None
            if url_attr in l:
                link = l[url_attr]
            elif opt_attr in l and ExtractLinksJob.url_abs_pattern.match(l[opt_attr]):
                link = l[opt_attr]
            else:
                continue
            # lurl = _url_join(base_url, urlparse(link)).geturl()
            try:
                lurl = urljoin(base_url, link)
            except ValueError:
                continue
            has_links = True
            yield src_url, lurl
        if not has_links:
            # ensure that every page is a node in the graph
            # even if it has not outgoing links
            yield src_url, src_url

    def yield_link(self, src, target):
        yield src, target

    def get_links(self, url, record):
        try:
            response_meta = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']
            if 'Headers' in response_meta:
                # extract links from HTTP header
                for l in self.yield_http_header_links(url, response_meta['Headers']):
                    yield l
            if 'HTML-Metadata' not in response_meta:
                self.records_non_html.add(1)
                return
            html_meta = response_meta['HTML-Metadata']
            base = None
            if 'Head' in html_meta:
                head = html_meta['Head']
                if 'Base' in head:
                    try:
                        base = urljoin(url, head['Base'])
                    except ValueError:
                        pass
                if 'Link' in head:
                    # <link ...>
                    for l in self.yield_links(url, base, head['Link'], 'url'):
                        yield l
                if 'Metas' in head:
                    for m in head['Metas']:
                        if (('property' in m and m['property']
                             in ExtractLinksJob.html_meta_property_links)
                            or ('name' in m and m['name']
                                in ExtractLinksJob.html_meta_links)
                            or ('content' in m
                                and ExtractLinksJob.url_abs_pattern.match(m['content']))):
                            for l in self.yield_links(url, base, [m], 'content'):
                                yield l
                if 'Scripts' in head:
                    for l in self.yield_links(url, base, head['Scripts'], 'url'):
                        yield l
            if 'Links' in html_meta:
                for l in self.yield_links(url, base, html_meta['Links'],
                                          'url', 'href'):
                    yield l

        except KeyError as e:
            self.get_logger().error("Failed to parse record for {}: {}".format(
                url, e))
            self.records_failed.add(1)

    def init_accumulators(self, session):
        super(ExtractLinksJob, self).init_accumulators(session)

        sc = session.sparkContext
        self.records_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.records_response = sc.accumulator(0)
        self.records_response_wat = sc.accumulator(0)
        self.records_response_warc = sc.accumulator(0)
        self.records_response_redirect = sc.accumulator(0)
        self.records_response_robotstxt = sc.accumulator(0)
        self.link_count = sc.accumulator(0)

    def log_accumulators(self, session):
        super(ExtractLinksJob, self).log_accumulators(session)

        self.log_accumulator(session, self.records_response,
                             'response records = {}')
        self.log_accumulator(session, self.records_failed,
                             'records failed to process = {}')
        self.log_accumulator(session, self.records_non_html,
                             'records not HTML = {}')
        self.log_accumulator(session, self.records_response_wat,
                             'response records WAT = {}')
        self.log_accumulator(session, self.records_response_warc,
                             'response records WARC = {}')
        self.log_accumulator(session, self.records_response_redirect,
                             'response records redirects = {}')
        self.log_accumulator(session, self.records_response_robotstxt,
                             'response records robots.txt = {}')
        self.log_accumulator(session, self.link_count,
                             'non-unique link pairs = {}')

    def run_job(self, session):
        output = None
        if self.args.input != '':
            input_data = session.sparkContext.textFile(
                self.args.input,
                minPartitions=self.args.num_input_partitions)
            output = input_data.mapPartitionsWithIndex(self.process_warcs)

        if not self.args.intermediate_output:
            df = session.createDataFrame(output, schema=self.output_schema)
        else:
            if output is not None:
                session.createDataFrame(output, schema=self.output_schema) \
                    .write \
                    .format(self.args.output_format) \
                    .option("compression", self.args.output_compression) \
                    .saveAsTable(self.args.intermediate_output)
                self.log_accumulators(session.sparkContext)
            warehouse_dir = session.conf.get('spark.sql.warehouse.dir',
                                             'spark-warehouse')
            intermediate_output = os.path.join(warehouse_dir,
                                               self.args.intermediate_output)
            df = session.read.parquet(intermediate_output)

        df.dropDuplicates() \
          .coalesce(self.args.num_output_partitions) \
          .sortWithinPartitions('s', 't') \
          .write \
          .format(self.args.output_format) \
          .option("compression", self.args.output_compression) \
          .saveAsTable(self.args.output)

        self.log_accumulators(session.sparkContext)


class ExtractHostLinksJob(ExtractLinksJob):
    """Extract links from WAT files, redirects from WARC files,
    and sitemap links from robots.txt response records.
    Extract the host names, reverse the names (example.com -> com.example)
    and save the pairs <source_host, target_host>."""

    name = "ExtrHostLinks"
    output_schema = StructType([
        StructField("s", StringType(), True),
        StructField("t", StringType(), True)
    ])
    num_input_partitions = 32
    num_output_partitions = 16

    # match global links
    # - with URL scheme, more restrictive than specified in
    #   https://tools.ietf.org/html/rfc3986#section-3.1
    # - or starting with //
    #   (all other "relative" links are within the same host)
    global_link_pattern = re.compile(r'^(?:[a-z][a-z0-9]{1,5}:)?//',
                                     re.IGNORECASE|re.ASCII)

    # match IP addresses
    # - including IPs with leading `www.' (stripped)
    ip_pattern = re.compile(r'^(?:www\.)?\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\Z')

    # valid host names, relaxed allowing underscore, allowing also IDNAs
    # https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_hostnames
    host_part_pattern = re.compile(r'^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?\Z',
                                   re.IGNORECASE|re.ASCII)

    # simple pattern to match many but not all host names in URLs
    url_parse_host_pattern = re.compile(r'^https?://([a-z0-9_.-]{2,253})(?:[/?#]|\Z)',
                                        re.IGNORECASE|re.ASCII)

    @staticmethod
    def get_surt_host(url):
        m = ExtractHostLinksJob.url_parse_host_pattern.match(url)
        if m:
            host = m.group(1)
        else:
            try:
                host = urlparse(url).hostname
            except Exception as e:
                # self.get_logger().debug("Failed to parse URL {}: {}\n".format(url, e))
                return None
            if not host:
                return None
        host = host.strip().lower()
        if len(host) < 1 or len(host) > 253:
            return None
        if ExtractHostLinksJob.ip_pattern.match(host):
            return None
        parts = host.split('.')
        if parts[-1] == '':
            # trailing dot is allowed, strip it
            parts = parts[0:-1]
        if len(parts) <= 1:
            # do not accept single-word hosts, must be at least `domain.tld'
            return None
        if len(parts) > 2 and parts[0] == 'www':
            # strip leading 'www' to reduce number of "duplicate" hosts,
            # but leave at least 2 trailing parts (www.com is a valid domain)
            parts = parts[1:]
        for (i, part) in enumerate(parts):
            if len(part) > 63:
                return None
            if not ExtractHostLinksJob.host_part_pattern.match(part):
                try:
                    idn = idna.encode(part).decode('ascii')
                except (idna.IDNAError, idna.core.InvalidCodepoint, UnicodeError, IndexError, Exception):
                    # self.get_logger().debug("Invalid host name: {}".format(url))
                    return None

                # TODO: idna verifies the resulting string for length restrictions or invalid chars,
                #       maybe no further verification is required:
                if ExtractHostLinksJob.host_part_pattern.match(idn):
                    parts[i] = idn
                else:
                    # self.get_logger().debug("Invalid host name: {}".format(url))
                    return None
        parts.reverse()
        return '.'.join(parts)

    def yield_links(self, src_url, base_url, links, url_attr, opt_attr=None,
                    src_host=None, base_host=None):
        if not src_host:
            src_host = ExtractHostLinksJob.get_surt_host(src_url)
        if base_url and not base_host:
            base_host = ExtractHostLinksJob.get_surt_host(base_url)
        if base_host and not src_host:
            src_host = base_host
        if not src_host:
            return
        target_hosts = set()
        inner_host_links = 0
        for l in links:
            if not l:
                continue
            if url_attr in l:
                link = l[url_attr]
            elif opt_attr in l and ExtractLinksJob.url_abs_pattern.match(l[opt_attr]):
                link = l[opt_attr]
            else:
                continue
            if self.global_link_pattern.match(link):
                try:
                    thost = ExtractHostLinksJob.get_surt_host(link)
                    if not thost:
                        pass  # no host, e.g., http:///abc/, file:///C:...
                    elif thost == src_host:
                        pass  # global link to same host
                    else:
                        target_hosts.add(thost)
                except ValueError:
                    pass
            else:
                inner_host_links += 1
        for t in target_hosts:
            yield src_host, t
        if inner_host_links > 0 and base_url is not None:
            if base_host and base_host != src_host:
                # any internal link becomes an external link
                yield src_host, base_host

    def yield_link(self, src, target):
        src_host = ExtractHostLinksJob.get_surt_host(src)
        thost = ExtractHostLinksJob.get_surt_host(target)
        if thost and src_host:
            yield src_host, thost

    def yield_http_header_links(self, url, headers, src_host=None):
        links = []
        if 'Content-Location' in headers:
            links.append(headers['Content-Location'])
        if 'Link' in headers:
            for m in ExtractLinksJob.http_link_pattern.finditer(headers['Link']):
                links.append(m.group(1))
        if links:
            if not src_host:
                src_host = ExtractHostLinksJob.get_surt_host(url)
                if not src_host:
                    return
            for link in links:
                host = ExtractHostLinksJob.get_surt_host(link)
                if host is not None and src_host != host:
                    yield src_host, host

    def get_links(self, url, record):
        try:
            response_meta = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']
            src_host = ExtractHostLinksJob.get_surt_host(url)
            if src_host:
                if 'Headers' in response_meta:
                    # extract links from HTTP header
                    for l in self.yield_http_header_links(url, response_meta['Headers'],
                                                          src_host=src_host):
                        yield l
            if 'HTML-Metadata' not in response_meta:
                self.records_non_html.add(1)
                return
            html_meta = response_meta['HTML-Metadata']
            base = None
            base_host = None
            if 'Head' in html_meta:
                head = html_meta['Head']
                if 'Base' in head:
                    try:
                        base = urljoin(url, head['Base'])
                        base_host = ExtractHostLinksJob.get_surt_host(base)
                    except ValueError:
                        pass
                if 'Link' in head:
                    # <link ...>
                    for l in self.yield_links(url, base, head['Link'], 'url',
                                              src_host=src_host, base_host=base_host):
                        yield l
                if 'Metas' in head:
                    for m in head['Metas']:
                        if (('property' in m and m['property']
                             in ExtractLinksJob.html_meta_property_links)
                            or ('name' in m and m['name']
                                in ExtractLinksJob.html_meta_links)
                            or ('content' in m
                                and ExtractLinksJob.url_abs_pattern.match(m['content']))):
                            for l in self.yield_links(url, base, [m], 'content',
                                                      src_host=src_host, base_host=base_host):
                                yield l
                if 'Scripts' in head:
                    for l in self.yield_links(url, base, head['Scripts'], 'url',
                                              src_host=src_host, base_host=base_host):
                        yield l
            if 'Links' in html_meta:
                for l in self.yield_links(url, base, html_meta['Links'],
                                          'url', 'href',
                                          src_host=src_host, base_host=base_host):
                    yield l

        except KeyError as e:
            self.get_logger().error("Failed to parse record for {}: {}".format(
                url, e))
            self.records_failed.add(1)

    def process_robotstxt(self, record, stream, _http_status_line):
        """Process robots.txt and yield sitemap links"""
        line = stream.readline()
        while line:
            if line == b'\r\n':
                # end of HTTP header
                break
            line = stream.readline()
        line = stream.readline()
        while line:
            m = ExtractLinksJob.robotstxt_sitemap_pattern.match(line)
            if m:
                sitemap = m.group(1).strip()
                try:
                    sitemap = sitemap.decode('utf-8')
                    from_robotstxt = record.rec_headers.get_header('WARC-Target-URI')
                    src_host = ExtractHostLinksJob.get_surt_host(from_robotstxt)
                    thost = ExtractHostLinksJob.get_surt_host(sitemap)
                    if thost and src_host and src_host != thost:
                        yield src_host, thost
                except UnicodeError as e:
                    self.get_logger().warning(
                        'URL with unknown encoding: {} - {}'.format(
                            sitemap, e))
            line = stream.readline()


if __name__ == "__main__":
    # job = ExtractLinksJob()
    job = ExtractHostLinksJob()
    job.run()
