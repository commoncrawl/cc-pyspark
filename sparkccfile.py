import argparse
import json
import logging
import os
import re

from io import BytesIO
from tempfile import NamedTemporaryFile

import boto3
import botocore
import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'

class CCFileProcessorSparkJob(object):
    """
    A Spark job definition to process individual files from a manifest.
    This is a simplified version of sparkcc, where we only do the individual file downloads to a local temp file, and then process each file.
    (ie: it is much more generic, and should work in many more situations)
    """

    name = 'CCFileProcessor'

    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("val", LongType(), True)
    ])

    # description of input and output shown by --help
    input_descr = "Path to file listing input paths"
    output_descr = "Name of output table (saved in spark.sql.warehouse.dir)"

    # parse HTTP headers of WARC records (derived classes may override this)
    warc_parse_http_header = True

    args = None
    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    num_input_partitions = 400
    num_output_partitions = 10

    # S3 client is thread-safe, cf.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients)
    s3client = None

    # pattern to split a data URL (<scheme>://<netloc>/<path> or <scheme>:/<path>)
    data_url_pattern = re.compile('^(s3|https?|file|hdfs):(?://([^/]*))?/(.*)')


    def parse_arguments(self):
        """Returns the parsed arguments from the command line"""

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(prog=self.name, description=description,
                                             conflict_handler='resolve')

        arg_parser.add_argument("input", help=self.input_descr)
        arg_parser.add_argument("output", help=self.output_descr)

        arg_parser.add_argument("--input_base_url",
                                help="Base URL (prefix) used if paths to WARC/WAT/WET "
                                "files are relative paths. Used to select the "
                                "access method: s3://commoncrawl/ (authenticated "
                                "S3) or https://data.commoncrawl.org/ (HTTP)")
        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions, "
                                "number of parallel tasks to process WARC "
                                "files/records")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
                                help="Number of output partitions")
        arg_parser.add_argument("--output_format", default="parquet",
                                help="Output format: parquet (default),"
                                " orc, json, csv")
        arg_parser.add_argument("--output_compression", default="gzip",
                                help="Output compression codec: None,"
                                " gzip/zlib (default), zstd, snappy, lzo, etc.")
        arg_parser.add_argument("--output_option", action='append', default=[],
                                help="Additional output option pair"
                                " to set (format-specific) output options, e.g.,"
                                " `header=true` to add a header line to CSV files."
                                " Option name and value are split at `=` and"
                                " multiple options can be set by passing"
                                " `--output_option <name>=<value>` multiple times")

        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                " buffer content from S3")

        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")
        arg_parser.add_argument("--spark-profiler", action='store_true',
                                help="Enable PySpark profiler and log"
                                " profiling metrics if job has finished,"
                                " cf. spark.python.profile")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args()
        self.init_logging(args.log_level)
        if not self.validate_arguments(args):
            raise Exception("Arguments not valid")

        return args

    def add_arguments(self, parser):
        """Allows derived classes to add command-line arguments.
           Derived classes overriding this method must call
           super().add_arguments(parser) in order to add "register"
           arguments from all classes in the hierarchy."""
        pass

    def validate_arguments(self, args):
        """Validate arguments. Derived classes overriding this method
           must call super().validate_arguments(args)."""
        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"
        return True

    def get_output_options(self):
        """Convert output options strings (opt=val) to kwargs"""
        return {x[0]: x[1] for x in map(lambda x: x.split('=', 1),
                                        self.args.output_option)}

    def init_logging(self, level=None, session=None):
        if level:
            self.log_level = level
        else:
            level = self.log_level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)
        logging.getLogger(self.name).setLevel(level)
        if session:
            session.sparkContext.setLogLevel(level)

    def init_accumulators(self, session):
        """Register and initialize counters (aka. accumulators).
           Derived classes may use this method to add their own
           accumulators but must call super().init_accumulators(session)
           to also initialize counters from base classes."""
        sc = session.sparkContext
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def get_logger(self, session=None):
        """Get logger from SparkSession or (if None) from logging module"""
        if not session:
            try:
                session = SparkSession.getActiveSession()
            except AttributeError:
                pass # method available since Spark 3.0.0
        if session:
            return session._jvm.org.apache.log4j.LogManager \
                        .getLogger(self.name)
        return logging.getLogger(self.name)

    def run(self):
        """Run the job"""
        self.args = self.parse_arguments()

        builder = SparkSession.builder.appName(self.name)

        if self.args.spark_profiler:
            builder.config("spark.python.profile", "true")

        session = builder.getOrCreate()

        self.init_logging(self.args.log_level, session)
        self.init_accumulators(session)

        self.run_job(session)

        if self.args.spark_profiler:
            session.sparkContext.show_profiles()

        session.stop()

    def log_accumulator(self, session, acc, descr):
        """Log single counter/accumulator"""
        self.get_logger(session).info(descr.format(acc.value))

    def log_accumulators(self, session):
        """Log counters/accumulators, see `init_accumulators`."""
        self.log_accumulator(session, self.warc_input_processed,
                             'WARC/WAT/WET input files processed = {}')
        self.log_accumulator(session, self.warc_input_failed,
                             'WARC/WAT/WET input files failed = {}')
        self.log_accumulator(session, self.records_processed,
                             'WARC/WAT/WET records processed = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    def run_job(self, session):
        input_data = session.sparkContext.textFile(self.args.input,
                                                   minPartitions=self.args.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_files) \
            .reduceByKey(self.reduce_by_key_func)

        session.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.args.output)

        self.log_accumulators(session)

    def get_s3_client(self):
        if not self.s3client:
            self.s3client = boto3.client('s3', use_ssl=False)
        return self.s3client

    def fetch_file(self, uri, base_uri=None):
        """Fetch file"""

        (scheme, netloc, path) = (None, None, None)
        uri_match = self.data_url_pattern.match(uri)
        if not uri_match and base_uri:
            # relative input URI (path) and base URI defined
            uri = base_uri + uri
            uri_match = self.data_url_pattern.match(uri)

        if uri_match:
            (scheme, netloc, path) = uri_match.groups()
        else:
            # keep local file paths as is
            path = uri

        warctemp = None

        if scheme == 's3':
            bucketname = netloc
            if not bucketname:
                self.get_logger().error("Invalid S3 URI: " + uri)
                return
            if not path:
                self.get_logger().error("Empty S3 path: " + uri)
                return
            elif path[0] == '/':
                # must strip leading / in S3 path
                path = path[1:]
            self.get_logger().info('Reading from S3 {}'.format(uri))
            # download entire file using a temporary file for buffering
            warctemp = NamedTemporaryFile(mode='w+b', dir=self.args.local_temp_dir)
            try:
                self.get_s3_client().download_fileobj(bucketname, path, warctemp)
                warctemp.flush()
                warctemp.seek(0)
            except botocore.client.ClientError as exception:
                self.get_logger().error(
                    'Failed to download {}: {}'.format(uri, exception))
                self.warc_input_failed.add(1)
                warctemp.close()

        elif scheme == 'http' or scheme == 'https':
            headers = None
            self.get_logger().info('Fetching {}'.format(uri))
            response = requests.get(uri, headers=headers)

            if response.ok:
                # includes "HTTP 206 Partial Content" for range requests
                warctemp = NamedTemporaryFile(  mode='w+b',
                                                dir=self.args.local_temp_dir)
                warctemp.write(response.content)
                warctemp.flush()
                warctemp.seek(0)
            else:
                self.get_logger().error(
                    'Failed to download {}: {}'.format(uri, response.status_code))

        else:
            self.get_logger().info('Reading local file {}'.format(uri))
            if scheme == 'file':
                # must be an absolute path
                uri = os.path.join('/', path)
            else:
                base_dir = os.path.abspath(os.path.dirname(__file__))
                uri = os.path.join(base_dir, uri)
            warctemp = open(uri, 'rb')

        return warctemp

    def process_files(self, _id, iterator):
        """Process files, calling process_file(...) for each file"""
        for uri in iterator:
            self.warc_input_processed.add(1)

            tempfd = self.fetch_file(uri, self.args.input_base_url)
            if not tempfd:
                continue

            for res in self.process_file(uri, tempfd):
                yield res
            
            tempfd.close()