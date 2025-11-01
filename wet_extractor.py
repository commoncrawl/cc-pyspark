import json

from datetime import datetime
from io import BytesIO
from tempfile import SpooledTemporaryFile

from fastwarc.warc import WarcRecordType
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from resiliparse_parser import HTMLParser
from warcio import WARCWriter

from sparkcc import CCFileProcessorSparkJob
from sparkcc_fastwarc import CCFastWarcSparkJob


class WETExtractor(CCFastWarcSparkJob, CCFileProcessorSparkJob):
    """Experimental WET extractor, using FastWARC and Resiliparse.
    WET files are placed in the directory specified by `--output_base_url`.
    The output table contains URIs and their respective WET record locations:
    filename, offset, and length of the WET record.

    Cf. https://github.com/commoncrawl/ia-web-commons/blob/master/src/main/java/org/archive/extract/WETExtractorOutput.java
    """

    name = "WETExtractor"

    output_schema = StructType([
        StructField("uri", StringType(), True),
        StructField("wet_record_location", StructType([
            StructField("filename", StringType(), True),
            StructField("offset", IntegerType(), True),
            StructField("length", IntegerType(), True)
        ]), True)
    ])

    fastwarc_record_filter = WarcRecordType.warcinfo | WarcRecordType.response | WarcRecordType.metadata

    def __init__(self):
        self.html_parser = HTMLParser()

    def init_accumulators(self, session):
        super(WETExtractor, self).init_accumulators(session)

        sc = session.sparkContext
        self.warc_input_skip_output_exists = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.wet_records_written = sc.accumulator(0)

    def log_accumulators(self, session):
        super(WETExtractor, self).log_accumulators(session)

        self.log_accumulator(session, self.warc_input_skip_output_exists,
                             'skipped WARC input, WET output already exists = {}')
        self.log_accumulator(session, self.records_non_html,
                             'records not HTML = {}')
        self.log_accumulator(session, self.wet_records_written,
                             'WET records written = {}')

    def create_warcinfo_record(self, wet_writer, wet_file_name, record, content):
        wet_headers = {
            'Software-Info':
                'cc-pyspark wet_extractor.py based on FastWARC, Resiliparse, warcio',
            'Extracted-Date':
                datetime.utcnow()
        }
        wet_headers_from_warc = {
            'robots', 'ispartof', 'operator', 'description', 'publisher'}
        if self.is_warcinfo_record(record):
            # Add warcinfo fields from source WARC's warcinfo record
            try:
                content = content.decode('utf-8')
                for line in content.split('\r\n'):
                    if ':' in line:
                        name, value = line.split(':', 1)
                        if name.lower() in wet_headers_from_warc:
                            wet_headers[name] = value.strip()
            except Exception as e:
                self.get_logger().error('Error parsing warcinfo: %s', e)
        return wet_writer.create_warcinfo_record(wet_file_name, wet_headers)

    def create_and_write_wet_record(self, wet_writer, cache):
        """Write WET record using the data in cache.

        If there is a metadata record:
        - pass the identified character encoding to the HTML parser
        - extract identified languages metadata record and add as
          WARC header field "WARC-Identified-Content-Language
        """
        self.get_logger().debug('Converting %s', cache['uri'])
        if 'response' not in cache:
            self.get_logger().error('No response record for %s', cache['uri'])
            return
        elif cache['response'][1] is None:
            # no content because not HTML
            return
        encoding = None
        languages = None
        if 'metadata' in cache:
            try:
                content = cache['metadata'][1].decode('utf-8')
                for line in content.split('\r\n'):
                    if line.startswith('charset-detected:'):
                        _, value = line.split(':', 1)
                        encoding = value.strip()
                    elif line.startswith('languages-cld2:'):
                        _, value = line.split(':', 1)
                        lang_cld2 = json.loads(value.strip())
                        if 'languages' in lang_cld2:
                            languages = ','.join(map(lambda l: l['code-iso-639-3'],
                                                     lang_cld2['languages']))
            except Exception as e:
                self.get_logger().error('Error parsing metadata: %s', e)
        html_tree = self.html_parser.get_html_tree(cache['response'][1],
                                                   encoding=encoding)
        text = self.html_parser.html_to_text(html_tree)
        wet_headers_dict = {
            'WARC-Date': cache['date'],
            'WARC-Refers-To': cache['id'],
            'Content-Type': 'text/plain',
        }
        if languages:
            wet_headers_dict['WARC-Identified-Content-Language'] = languages
        wet_record = wet_writer.create_warc_record(
            cache['uri'], 'conversion',
            payload=BytesIO(str.encode(text, 'UTF-8')),
            warc_headers_dict=wet_headers_dict)
        wet_writer.write_record(wet_record)

    def process_record(self, record):
        if self.is_warcinfo_record(record) or self.is_metadata_record(record):
            yield record, self.get_payload_stream(record).read()
        if not self.is_response_record(record):
            # skip over WARC request records
            return
        if not self.is_html(record):
            # non-HTML record: yield without content
            self.records_non_html.add(1)
            yield record, None
            return
        yield record, self.get_payload_stream(record).read()

    def process_warc(self, uri, stream):
        wet_file_name = uri.split('/')[-1].replace('.warc.gz', '.warc.wet.gz')
        if self.check_for_output_file(wet_file_name, self.args.output_base_url):
            self.warc_input_skip_output_exists.add(1)
            return
        with SpooledTemporaryFile(max_size=524288,
                                  mode='w+b',
                                  dir=self.args.local_temp_dir) as wet_temp:
            wet_writer = WARCWriter(wet_temp, gzip=True)
            offset = wet_temp.tell()
            cache = {'uri': None, 'date': None}
            for res in super(WETExtractor, self).process_warc(uri, stream):
                (record, content) = res
                if offset == 0:
                    wet_writer.write_record(
                        self.create_warcinfo_record(wet_writer, wet_file_name, record, content))
                    offset = wet_temp.tell()
                    if self.is_warcinfo_record(record):
                        continue
                warc_target_uri = self.get_warc_header(record, 'WARC-Target-URI')
                warc_date = self.get_warc_header(record, 'WARC-Date')
                if cache['uri'] and (cache['uri'] != warc_target_uri
                                     or cache['date'] != warc_date):
                    self.create_and_write_wet_record(wet_writer, cache)
                    end_offset = wet_temp.tell()
                    yield cache['uri'], (wet_file_name, offset, (end_offset-offset))
                    offset = end_offset
                    self.wet_records_written.add(1)
                    cache = {'uri': None, 'date': None}
                if cache['uri'] is None and cache['date'] is None:
                    cache['uri'] = warc_target_uri
                    cache['date'] = warc_date
                if self.is_response_record(record):
                    cache['response'] = (record, content)
                    cache['id'] = self.get_warc_header(record, 'WARC-Record-ID')
                elif self.is_metadata_record(record):
                    cache['metadata'] = (record, content)
            self.create_and_write_wet_record(wet_writer, cache)
            end_offset = wet_temp.tell()
            yield cache['uri'], (wet_file_name, offset, (end_offset-offset))
            wet_temp.seek(0)
            self.write_output_file(wet_file_name, wet_temp, self.args.output_base_url)

    def run_job(self, session):
        input_data = session.sparkContext.textFile(self.args.input,
                                                   minPartitions=self.args.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_warcs)

        session.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.args.output)

        self.log_accumulators(session)


if __name__ == '__main__':
    job = WETExtractor()
    job.run()
