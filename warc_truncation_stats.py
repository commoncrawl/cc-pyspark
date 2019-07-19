
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sparkcc import CCSparkJob


class TruncatedStatsJob(CCSparkJob):
    """Statistics about truncated content in WARC files"""

    name = "TruncatedStats"

    # output is <uri, trunc_reason, >
    output_schema = StructType([
        StructField("url", StringType(), False),
        StructField("warc_file", StringType(), False),
        StructField("warc_record_offset", IntegerType(), False),
        StructField("warc_record_length", IntegerType(), False),
        StructField("warc_content_length", IntegerType(), False),
        StructField("http_content_length", IntegerType(), True),
        StructField("payload_length", IntegerType(), True),
        StructField("http_orig_content_length", IntegerType(), True),
        StructField("http_orig_content_encoding", StringType(), True),
        StructField("http_orig_transfer_encoding", StringType(), True),
        StructField("truncated_reason", StringType(), True),
        StructField("http_content_type", StringType(), True),
        StructField("identified_payload_type", StringType(), True),
    ])

    content_limit = 2**20

    def add_arguments(self, parser):
        parser.add_argument("--output_option", action='append',
                            help="Additional output option pair"
                            "(split at `=`: <option_name>=<option_value>)")

    def iterate_records(self, _warc_uri, archive_iterator):
        """Iterate over all WARC records and aggregate statistics
           about (presumably) truncated WARC records"""
        for record in archive_iterator:
            if record.rec_type != 'response':
                continue

            truncated = False
            trunc_reason = ''
            if 'WARC-Truncated' in record.rec_headers:
                trunc_reason = record.rec_headers['WARC-Truncated']
                truncated = True
            http_content_length = int(record.http_headers.get_header(
                'content-length', -1))
            payload = record.content_stream().read()
            payload_length = len(payload)
            if http_content_length == TruncatedStatsJob.content_limit \
               or payload_length == TruncatedStatsJob.content_limit:
                # presumably truncated but not marked as such
                truncated = True

            if not truncated:
                continue

            url = record.rec_headers.get_header('WARC-Target-URI')
            warc_content_length = int(record.rec_headers['Content-Length'])
            identified_payload_type = ''
            if 'WARC-Identified-Payload-Type' in record.rec_headers:
                identified_payload_type = record.rec_headers[
                    'WARC-Identified-Payload-Type']
            http_content_type = record.http_headers.get_header(
                'content-type', '')
            http_orig_content_length = int(record.http_headers.get_header(
                'x-crawler-content-length', -1))
            http_orig_content_encoding = record.http_headers.get_header(
                'x-crawler-content-encoding', '')
            http_orig_transfer_encoding = record.http_headers.get_header(
                'x-crawler-transfer-encoding', '')
            warc_record_offset = archive_iterator.get_record_offset()
            warc_record_length = archive_iterator.get_record_length()
            warc_file = _warc_uri.split('/')[-1]

            self.records_processed.add(1)

            yield (url, warc_file, warc_record_offset, warc_record_length,
                   warc_content_length, http_content_length, payload_length,
                   http_orig_content_length, http_orig_content_encoding,
                   http_orig_transfer_encoding, trunc_reason,
                   http_content_type, identified_payload_type)

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_warcs)

        out = sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression)
        for output_option in self.args.output_option:
            (opt_name, opt_val) = output_option.split('=', 1)
            out = out.option(opt_name, opt_val)
        out.saveAsTable(self.args.output)

        self.log_aggregators(sc)


if __name__ == '__main__':
    job = TruncatedStatsJob()
    job.run()
