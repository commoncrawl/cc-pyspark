from io import BytesIO
from tempfile import SpooledTemporaryFile

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

    def process_record(self, record):
        if not self.is_response_record(record):
            # skip over WARC request or metadata records
            return ''
        if not self.is_html(record):
            self.records_non_html.add(1)
            return ''
        self.get_logger().debug('Converting %s',
                                self.get_warc_header(record, 'WARC-Target-URI'))
        page = self.get_payload_stream(record).read()
        encoding = self.get_warc_header(record, 'WARC-Identified-Content-Charset')
        html_tree = self.html_parser.get_html_tree(page, encoding=encoding)
        text = self.html_parser.html_to_text(html_tree)
        yield record, text

    def process_warc(self, uri, stream):
        wet_file_name = uri.split('/')[-1].replace('.warc.gz', '.warc.wet.gz')
        if self.check_for_output_file(wet_file_name, self.args.output_base_url):
            self.warc_input_skip_output_exists.add(1)
            return
        with SpooledTemporaryFile(max_size=524288,
                                  mode='w+b',
                                  dir=self.args.local_temp_dir) as wet_temp:
            wet_writer = WARCWriter(wet_temp, gzip=True)
            wet_writer.create_warcinfo_record(wet_file_name, {
                'Software-Info':
                    'cc-pyspark wet_extractor.py based on FastWARC, Resiliparse, warcio',
                # TODO: Add more warcinfo fields from source WARC's warcinfo record
            })
            offset = wet_temp.tell()
            for res in super(WETExtractor, self).process_warc(uri, stream):
                warc_target_uri = self.get_warc_header(res[0], 'WARC-Target-URI')
                warc_date = self.get_warc_header(res[0], 'WARC-Date')
                warc_record_id = self.get_warc_header(res[0], 'WARC-Record-ID')
                # TODO: extract language from following metadata record and add as
                #       WARC header field "WARC-Identified-Content-Language"
                wet_record = wet_writer.create_warc_record(
                    warc_target_uri, 'conversion',
                    payload=BytesIO(str.encode(res[1], 'UTF-8')),
                    warc_headers_dict={
                        'WARC-Date': warc_date,
                        'WARC-Refers-To': warc_record_id,
                        'Content-Type': 'text/plain',
                    })
                wet_writer.write_record(wet_record)
                end_offset = wet_temp.tell()
                yield warc_target_uri, (wet_file_name, offset, (end_offset-offset))
                offset = end_offset
                self.wet_records_written.add(1)
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
