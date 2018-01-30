import idna
import ujson as json
import os
import re

try:
    # Python2
    from urlparse import urljoin, urlparse
except ImportError:
    # Python3
    from urllib.parse import urljoin, urlparse

from sparkcc import CCSparkJob
from pyspark.sql.types import StructType, StructField, StringType


class ExtractLinksJob(CCSparkJob):
    '''Extract links from WAT files and redirects from WARC files'''
    ''' and save them as pairs <from, to>'''
    name = "ExtractLinks"

    output_schema = StructType([
        StructField("s", StringType(), True),
        StructField("t", StringType(), True)
    ])

    warc_parse_http_header = False

    records_response = None
    records_response_wat = None
    records_response_warc = None
    records_failed = None
    records_non_html = None
    records_response_redirect = None

    http_redirect_pattern = re.compile(b'^HTTP\s*/\s*1\.[01]\s*30[1278]\\b')
    http_redirect_location_pattern = re.compile(b'^Location:\s*(\S+)',
                                                re.IGNORECASE)

    def add_arguments(self, parser):
        parser.add_argument("--intermediate_output", type=str,
                            default=None,
                            help="Intermediate output to recover job from")

    @staticmethod
    def _url_join(base, link):
        # TODO: efficiently join without reparsing base
        # TODO: canonicalize
        pass

    def process_record(self, record):
        if self.is_wat_json_record(record):
            try:
                record = json.loads(record.content_stream().read())
            except ValueError as e:
                self.get_logger().error('Failed to load JSON: {}'.format(e))
                self.records_failed.add(1)
                return
            warc_header = record['Envelope']['WARC-Header-Metadata']
            if warc_header['WARC-Type'] != 'response':
                # WAT request or metadata records
                return
            self.records_response.add(1)
            self.records_response_wat.add(1)
            url = warc_header['WARC-Target-URI']
            for link in self.get_links(url, record):
                yield link
        elif record.rec_type == 'response':
            self.records_response.add(1)
            self.records_response_warc.add(1)
            stream = record.content_stream()
            http_status_line = stream.readline()
            if ExtractLinksJob.http_redirect_pattern.match(http_status_line):
                self.records_response_redirect.add(1)
            else:
                return
            line = stream.readline()
            while line:
                m = ExtractLinksJob.http_redirect_location_pattern.match(line)
                if m:
                    redir_to = m.group(1).strip()
                    try:
                        redir_to = redir_to.decode('utf-8')
                    except UnicodeError as e:
                        self.get_logger().warn(
                            'URL with unknown encoding: {} - {}'.format(
                                redir_to, e))
                    redir_from = record.rec_headers.get_header('WARC-Target-URI')
                    for link in self.yield_redirect(redir_from, redir_to,
                                                    http_status_line):
                        yield link
                    return
                elif line.strip() == '':
                    return
                line = stream.readline()

    def yield_redirect(self, src, target, http_status_line):
        if src != target:
            yield src, target

    def yield_links(self, from_url, base_url, links, url_attr='url'):
        # base_url = urlparse(base)
        if base_url is None:
            base_url = from_url
        for l in links:
            if url_attr in l:
                link = l[url_attr]
                # lurl = _url_join(base_url, urlparse(link)).geturl()
                try:
                    lurl = urljoin(base_url, link)
                except ValueError:
                    pass
                yield from_url, lurl

    def get_links(self, url, record):
        try:
            response_meta = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']
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
                    for l in self.yield_links(url, base, head['Link']):
                        yield l
                if 'Metas' in head:
                    for m in head['Metas']:
                        if 'property' in m and m['property'] == 'og:url':
                            for l in self.yield_links(url, base, [m], 'content'):
                                yield l
            if 'Links' in html_meta:
                for l in self.yield_links(url, base, html_meta['Links']):
                    yield l
        except KeyError as e:
            self.get_logger().error("Failed to parse record for {}: {}".format(
                url, e))
            self.records_failed.add(1)

    def log_aggregators(self, sc):
        # super(ExtractHostLinksJob, self).log_aggregators(sc)
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'records processed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'records processed = {}')

        self.log_aggregator(sc, self.records_response,
                            'response records = {}')
        self.log_aggregator(sc, self.records_failed,
                            'records failed to process = {}')
        self.log_aggregator(sc, self.records_non_html,
                            'records not HTML = {}')
        self.log_aggregator(sc, self.records_response_wat,
                            'response records WAT = {}')
        self.log_aggregator(sc, self.records_response_warc,
                            'response records WARC = {}')
        self.log_aggregator(sc, self.records_response_redirect,
                            'response records redirects = {}')

    def run_job(self, sc, sqlc):
        self.records_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.records_response = sc.accumulator(0)
        self.records_response_wat = sc.accumulator(0)
        self.records_response_warc = sc.accumulator(0)
        self.records_response_redirect = sc.accumulator(0)

        output = None
        if self.args.input != '':
            input_data = sc.textFile(
                self.args.input,
                minPartitions=self.args.num_input_partitions)
            output = input_data.mapPartitionsWithIndex(self.process_warcs)

        if self.args.intermediate_output is None:
            df = sqlc.createDataFrame(output, schema=self.output_schema)
        else:
            if output is not None:
                sqlc.createDataFrame(output, schema=self.output_schema) \
                    .write \
                    .format("parquet") \
                    .saveAsTable(self.args.intermediate_output)
                self.log_aggregators(sc)
            warehouse_dir = sc.getConf().get('spark.sql.warehouse.dir',
                                             'spark-warehouse')
            intermediate_output = os.path.join(warehouse_dir,
                                               self.args.intermediate_output)
            df = sqlc.read.parquet(intermediate_output)

        df.dropDuplicates() \
          .coalesce(self.args.num_output_partitions) \
          .sortWithinPartitions('s', 't') \
          .write \
          .format("parquet") \
          .saveAsTable(self.args.output)

        self.log_aggregators(sc)


class ExtractHostLinksJob(ExtractLinksJob):
    '''Extract links from WAT files and redirects from WARC files,
     extract the host names, reverse the names (example.com -> com.example)
     and save the pairs <from_host, to_host>.'''

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
    global_link_pattern = re.compile('^(?:[a-z][a-z0-9]{1,5}:)?//',
                                     re.IGNORECASE)

    # match IP addresses
    # - including IPs with leading `www.' (stripped)
    ip_pattern = re.compile('^(?:www\.)?\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\Z')

    # valid host names, relaxed allowing underscore, allowing also IDNs
    # https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_hostnames
    host_part_pattern = re.compile('^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?\Z',
                                   re.IGNORECASE)

    @staticmethod
    def get_surt_host(url):
        try:
            host = urlparse(url).hostname
        except:
            # self.get_logger().debug("Failed to parse URL {}: {}".format(url, e))
            return None
        if host is None:
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
        if parts[0] == 'www' and len(parts) > 1:
            # strip leading 'www' to reduce number of "duplicate" hosts
            parts = parts[1:]
        if len(parts) <= 1:
            # do not accept single-word hosts, must be at least `domain.tld'
            return None
        for i in range(0, len(parts)):
            part = parts[i]
            if not ExtractHostLinksJob.host_part_pattern.match(part):
                try:
                    idn = idna.encode(part).decode('ascii')
                except (idna.IDNAError, UnicodeError, IndexError, Exception):
                    # self.get_logger().debug("Invalid host name: {}".format(url))
                    return None

                if ExtractHostLinksJob.host_part_pattern.match(idn):
                    parts[i] = idn
                else:
                    # self.get_logger().debug("Invalid host name: {}".format(url))
                    return None
        parts.reverse()
        return '.'.join(parts)

    def yield_links(self, from_url, base_url, links, url_attr='url'):
        from_host = ExtractHostLinksJob.get_surt_host(from_url)
        if from_host is None:
            return
        target_hosts = set()
        inner_host_links = 0
        for l in links:
            if l is None:
                continue
            if url_attr in l:
                link = l[url_attr]
                if self.global_link_pattern.match(link):
                    try:
                        thost = ExtractHostLinksJob.get_surt_host(link)
                        if thost is None:
                            pass  # no host, e.g., http:///abc/, file:///C:...
                        else:
                            target_hosts.add(thost)
                    except ValueError:
                        pass
                else:
                    inner_host_links += 1
        for t in target_hosts:
            if t != from_host:
                yield from_host, t
        if inner_host_links > 0 and base_url is not None:
            base_host = ExtractHostLinksJob.get_surt_host(base_url)
            if base_host is not None and base_host != from_host:
                # any internal link becomes an external link
                yield from_host, base_host

    def yield_redirect(self, src, target, http_status_line):
        if src == target:
            return
        src_host = ExtractHostLinksJob.get_surt_host(src)
        thost = ExtractHostLinksJob.get_surt_host(target)
        if thost is None or src_host is None or src_host == thost:
            return
        yield src_host, thost


if __name__ == "__main__":
    # job = ExtractLinksJob()
    job = ExtractHostLinksJob()
    job.run()
