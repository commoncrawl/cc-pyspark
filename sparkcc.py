import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class CCSparkJob(object):
    """
    A simple Spark job definition to process Common Crawl data
    """

    name = 'CCSparkJob'

    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("val", LongType(), True)
    ])

    # description of input and output shown in --help
    input_descr = "Path to file listing input paths"
    output_descr = "Name of output table (saved in spark.sql.warehouse.dir)"

    warc_parse_http_header = True

    args = None
    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    num_input_partitions = 400
    num_output_partitions = 10

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(description=description)

        arg_parser.add_argument("input", help=self.input_descr)
        arg_parser.add_argument("output", help=self.output_descr)

        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
                                help="Number of output partitions")
        arg_parser.add_argument("--output_format", default="parquet",
                                help="Output format: parquet (default),"
                                " orc, json, csv")
        arg_parser.add_argument("--output_compression", default="gzip",
                                help="Output compression codec: None,"
                                " gzip/zlib (default), snappy, lzo, etc.")

        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                " buffer content from S3")

        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")
        arg_parser.add_argument("--spark-profiler", action='store_true',
                                help="Enable PySpark profiler")
        arg_parser.add_argument("--minimum_executors", type=int,
                                default=0,
                                help="sets spark.dynamicAllocation.minExecutors")
        arg_parser.add_argument("--maximum_executors", type=int,
                                default=0,
                                help="sets spark.dynamicAllocation.maxExecutors")
        arg_parser.add_argument("--maximize_resource_allocation", action='store_true',
                                help="sets maximizeResourceAllocation (EMR-specific option)")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args()
        if not self.validate_arguments(args):
            raise Exception("Arguments not valid")
        self.init_logging(args.log_level)

        return args

    def add_arguments(self, parser):
        pass

    def validate_arguments(self, args):
        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"
        return True

    def init_logging(self, level=None):
        if level is None:
            level = self.log_level
        else:
            self.log_level = level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        return spark_context._jvm.org.apache.log4j.LogManager \
            .getLogger(self.name)

    def run(self):
        self.args = self.parse_arguments()

        conf = SparkConf().setAll((
            ("spark.task.maxFailures", "10"),
            ("spark.locality.wait", "20s"),
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
        ))

        if self.args.spark_profiler:
            conf = conf.set("spark.python.profile", "true")
        if self.args.minimum_executors > 0:
            conf = conf.set("spark.dynamicAllocation.enabled", "true")
            conf = conf.set("spark.dynamicAllocation.minExecutors", self.args.minimum_executors)
        if self.args.maximum_executors > 0:
            conf = conf.set("spark.dynamicAllocation.enabled", "true")
            conf = conf.set("spark.dynamicAllocation.maxExecutors", self.args.maximum_executors)
        if self.args.maximize_resource_allocation:
            conf = conf.set("maximizeResourceAllocation", "true")

        sc = SparkContext(
            appName=self.name,
            conf=conf)
        sqlc = SQLContext(sparkContext=sc)

        self.init_accumulators(sc)

        self.run_job(sc, sqlc)
        if self.args.spark_profiler:
            sc.show_profiles()

        sc.stop()

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC/WAT/WET input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'WARC/WAT/WET input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'WARC/WAT/WET records processed = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_warcs) \
            .reduceByKey(self.reduce_by_key_func)

        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .saveAsTable(self.args.output)

        self.log_aggregators(sc)

    def process_warcs(self, id_, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        for uri in iterator:
            self.warc_input_processed.add(1)
            if uri.startswith('s3://'):
                self.get_logger().info('Reading from S3 {}'.format(uri))
                s3match = s3pattern.match(uri)
                if s3match is None:
                    self.get_logger().error("Invalid S3 URI: " + uri)
                    continue
                bucketname = s3match.group(1)
                path = s3match.group(2)
                warctemp = TemporaryFile(mode='w+b',
                                         dir=self.args.local_temp_dir)
                try:
                    s3client.download_fileobj(bucketname, path, warctemp)
                except botocore.client.ClientError as exception:
                    self.get_logger().error(
                        'Failed to download {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    warctemp.close()
                    continue
                warctemp.seek(0)
                stream = warctemp
            elif uri.startswith('hdfs://'):
                self.get_logger().error("HDFS input not implemented: " + uri)
                continue
            else:
                self.get_logger().info('Reading local stream {}'.format(uri))
                if uri.startswith('file:'):
                    uri = uri[5:]
                uri = os.path.join(base_dir, uri)
                try:
                    stream = open(uri, 'rb')
                except IOError as exception:
                    self.get_logger().error(
                        'Failed to open {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    continue

            no_parse = (not self.warc_parse_http_header)
            try:
                archive_iterator = ArchiveIterator(stream,
                                                   no_record_parse=no_parse)
                for res in self.iterate_records(uri, archive_iterator):
                    yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(uri, exception))
            finally:
                stream.close()

    def process_record(self, record):
        raise NotImplementedError('Processing record needs to be customized')

    def iterate_records(self, _warc_uri, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res
            self.records_processed.add(1)
            # WARC record offset and length should be read after the record
            # has been processed, otherwise the record content is consumed
            # while offset and length are determined:
            #  warc_record_offset = archive_iterator.get_record_offset()
            #  warc_record_length = archive_iterator.get_record_length()

    @staticmethod
    def is_wet_text_record(record):
        """Return true if WARC record is a WET text/plain record"""
        return (record.rec_type == 'conversion' and
                record.content_type == 'text/plain')

    @staticmethod
    def is_wat_json_record(record):
        """Return true if WARC record is a WAT record"""
        return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

    @staticmethod
    def is_html(record):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.rec_headers) and
            (record.rec_headers['WARC-Identified-Payload-Type'] in
             html_types)):
            return True
        for html_type in html_types:
            if html_type in record.content_type:
                return True
        return False


class CCIndexSparkJob(CCSparkJob):
    """
    Process the Common Crawl columnar URL index
    """

    name = "CCIndexSparkJob"

    # description of input and output shown in --help
    input_descr = "Path to Common Crawl index table"

    def add_arguments(self, parser):
        parser.add_argument("--query", default=None,
                            help="SQL query to select rows. Note: the result "
                            "is required to contain the columns `url', `warc"
                            "_filename', `warc_record_offset' and `warc_record"
                            "_length', make sure they're SELECTed.")
        parser.add_argument("--csv", default=None,
                            help="CSV file to load WARC records by filename, "
                            "offset and length. The CSV file must have column "
                            "headers and the input columns `url', `warc"
                            "_filename', `warc_record_offset' and `warc_record"
                            "_length' are mandatory, see also option --query.")
        parser.add_argument("--table", default="ccindex",
                            help="name of the table data is loaded into"
                            " (default: ccindex)")

    def validate_arguments(self, args):
        if args.csv is None and args.query is None:
            self.get_logger().error(
                "One of options --csv or --query is required.")
            return False
        if args.csv is not None and args.query is not None:
            self.get_logger().error(
                "Options --csv and --query are mutually exclusive.")
            return False
        return super(CCIndexSparkJob, self).validate_arguments(args)

    def load_table(self, sc, spark, table_path, table_name):
        df = spark.read.load(table_path)
        df.createOrReplaceTempView(table_name)
        self.get_logger(sc).info(
            "Schema of table {}:\n{}".format(table_name, df.schema))

    def execute_query(self, sc, spark, query):
        sqldf = spark.sql(query)
        self.get_logger(sc).info("Executing query: {}".format(query))
        sqldf.explain()
        return sqldf

    def load_dataframe(self, sc, partitions=-1):
        session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        if self.args.query is not None:
            self.load_table(sc, session, self.args.input, self.args.table)
            sqldf = self.execute_query(sc, session, self.args.query)
        else:
            sqldf = session.read.format("csv").option("header", True) \
                .option("inferSchema", True).load(self.args.csv)
        sqldf.persist()

        num_rows = sqldf.count()
        self.get_logger(sc).info(
            "Number of records/rows matched by query: {}".format(num_rows))

        if partitions > 0:
            self.get_logger(sc).info(
                "Repartitioning data to {} partitions".format(partitions))
            sqldf = sqldf.repartition(partitions)

        return sqldf

    def run_job(self, sc, sqlc):
        sqldf = self.load_dataframe(sc, self.args.num_output_partitions)

        sqldf.write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .saveAsTable(self.args.output)

        self.log_aggregators(sc)


class CCIndexWarcSparkJob(CCIndexSparkJob):
    """
    Process Common Crawl data (WARC records) found by the columnar URL index
    """

    name = "CCIndexWarcSparkJob"

    def fetch_process_warc_records(self, rows):
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)
        bucketname = "commoncrawl"
        no_parse = (not self.warc_parse_http_header)

        for row in rows:
            url = row[0]
            warc_path = row[1]
            offset = int(row[2])
            length = int(row[3])
            self.get_logger().debug("Fetching WARC record for {}".format(url))
            rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
            try:
                response = s3client.get_object(Bucket=bucketname,
                                               Key=warc_path,
                                               Range=rangereq)
            except botocore.client.ClientError as exception:
                self.get_logger().error(
                    'Failed to download: {} ({}, offset: {}, length: {}) - {}'
                    .format(url, warc_path, offset, length, exception))
                self.warc_input_failed.add(1)
                continue
            record_stream = BytesIO(response["Body"].read())
            try:
                for record in ArchiveIterator(record_stream,
                                              no_record_parse=no_parse):
                    for res in self.process_record(record):
                        yield res
                    self.records_processed.add(1)
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC record: {} ({}, offset: {}, length: {}) - {}'
                    .format(url, warc_path, offset, length, exception))

    def run_job(self, sc, sqlc):
        sqldf = self.load_dataframe(sc, self.args.num_input_partitions)

        warc_recs = sqldf.select("url", "warc_filename", "warc_record_offset",
                                 "warc_record_length").rdd

        output = warc_recs.mapPartitions(self.fetch_process_warc_records) \
            .reduceByKey(self.reduce_by_key_func)

        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .saveAsTable(self.args.output)

        self.log_aggregators(sc)
