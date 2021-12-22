from collections import Counter

from bs4.dammit import EncodingDetector

from sparkcc import CCIndexWarcSparkJob
from grep import GrepJob


class CCIndexGrepJob(GrepJob, CCIndexWarcSparkJob):
    """Search for regular expression patterns in Common Crawl WARC
       records and count the matched strings and matched pages.
       WARC records are selected ahead via a SQL query on the
       columnar URL index."""

    name = "CCIndexGrep"

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        patterns = self.byte_patterns
        data = record.content_stream().read()
        encoding = record.rec_headers['WARC-Identified-Content-Charset']
        if not encoding and self.is_html(record):
            for encoding in EncodingDetector(data, is_html=True).encodings:
                # take the first detected encoding
                break
        if encoding:
            try:
                data = data.decode(encoding)
                encoding = None
                patterns = self.str_patterns
            except Exception as e:
                url = record.rec_headers.get_header('WARC-Target-URI')
                self.get_logger().warn("Failed to decode {}: {}".format(url, e))
                encoding = 'iso-8859-1'
        else:
            encoding = 'iso-8859-1' # used to convert binary data to string
        matches = GrepJob.match_patterns(patterns, data, encoding)
        for m, count in Counter(matches).items():
            yield m, (count, 1)


if __name__ == '__main__':
    job = CCIndexGrepJob()
    job.run()
