from collections import Counter

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from sparkcc import CCIndexWarcSparkJob
from word_count import WordCountJob


class CCIndexWordCountJob(WordCountJob, CCIndexWarcSparkJob):
    """ Word count (frequency list) from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexWordCount"

    records_parsing_failed = None
    records_non_html = None

    def init_accumulators(self, session):
        super(CCIndexWordCountJob, self).init_accumulators(session)

        sc = session.sparkContext
        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_accumulators(self, session):
        super(CCIndexWordCountJob, self).log_accumulators(session)

        self.log_accumulator(session, self.records_parsing_failed,
                             'records failed to parse = {}')
        self.log_accumulator(session, self.records_non_html,
                             'records not HTML = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        # sum values of tuple <term_frequency, document_frequency>
        return ((a[0] + b[0]), (a[1] + b[1]))

    def process_record(self, record):
        if not self.is_response_record(record):
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            self.records_non_html.add(1)
            return

        text = ""
        try:
            page = self.get_payload_stream(record).read()
            encoding = self.get_warc_header(record, 'WARC-Identified-Content-Charset')
            parser = self.get_html_parser()
            html_tree = parser.get_html_tree(page, encoding=encoding)
            text = parser.html_to_text(html_tree)
        except Exception as e:
            self.get_logger().error("Error converting HTML to text for {}: {}",
                                    self.get_warc_header(record, 'WARC-Target-URI'), e)
            self.records_parsing_failed.add(1)
        words = map(lambda w: w.lower(),
                    WordCountJob.word_pattern.findall(text))
        for word, count in Counter(words).items():
            yield word, (count, 1)


if __name__ == '__main__':
    job = CCIndexWordCountJob()
    job.run()
