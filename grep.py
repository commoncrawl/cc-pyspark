import re

from collections import Counter

from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob


class GrepJob(CCSparkJob):
    """Search for regular expression patterns in Common Crawl WARC
       or WET files and count the matched strings and matched pages"""

    name = "Grep"

    # output is <match, <term_frequency, document_frequency>>
    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("val", StructType([
            StructField("tf", LongType(), True),
            StructField("df", LongType(), True)]), True)
    ])

    def add_arguments(self, parser):
        super(GrepJob, self).add_arguments(parser)
        parser.add_argument("-e", "--regexp", required=True,
                            action='append', default=[],
                            help="Regular expression pattern(s) to search.")
        # TODO: behave similar to Unix "grep":
        #   - current implementation counts the matched strings, equiv.
        #      grep -o -e <pattern> <input_files> | sort | uniq -c
        #   - should provide options to:
        #     - output matched records (URL, WARC filename + offsets)
        #     - context of matches (line, fixed amount of bytes for HTML)
        #     - ???
        # parser.add_argument("-o", "--only-matching", action='store_true',
        #                     help="Only show the matches together with the "
        #                     "total number of matches and the number of "
        #                     "matched pages.")

    def validate_arguments(self, args):
        self.str_patterns = []
        self.byte_patterns = []
        for pattern in args.regexp:
            try:
                self.str_patterns.append(re.compile(pattern))
                self.byte_patterns.append(
                    re.compile(pattern.encode('iso-8859-1')))
            except Exception as e:
                self.get_logger().error(
                    "Invalid pattern `{}`: {}".format(pattern, e))
                return False
        return True

    @staticmethod
    def reduce_by_key_func(a, b):
        # sum values of tuple <term_frequency, document_frequency>
        return ((a[0] + b[0]), (a[1] + b[1]))

    @staticmethod
    def match_patterns(patterns, data, encoding):
        matches = []
        for p in patterns:
            for m in p.findall(data):
                if p.groups > 1:
                    # handle patterns with multiple capturing groups
                    # resulting in a tuple of captured strings
                    if encoding:
                        matches.append('\t'.join(map(lambda b: b.decode(encoding), m)))
                    else:
                        matches.append('\t'.join(m))
                else:
                    if encoding:
                        matches.append(m.decode(encoding))
                    else:
                        matches.append(m)
        return matches

    def process_record(self, record):
        encoding = 'iso-8859-1' # used for binary HTML
        patterns = self.byte_patterns
        data = record.content_stream().read()
        if self.is_wet_text_record(record):
            encoding = None
            patterns = self.str_patterns
            data = data.decode('utf-8')
        matches = GrepJob.match_patterns(patterns, data, encoding)
        for m, count in Counter(matches).items():
            yield m, (count, 1)


if __name__ == '__main__':
    job = GrepJob()
    job.run()
