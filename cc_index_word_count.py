from collections import Counter

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from sparkcc import CCIndexWarcSparkJob
from word_count import WordCountJob


class CCIndexWordCountJob(CCIndexWarcSparkJob, WordCountJob):
    """ Word count (frequency list) from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexWordCount"

    records_parsing_failed = None
    records_non_html = None

    def init_accumulators(self, sc):
        super(CCIndexWordCountJob, self).init_accumulators(sc)

        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_aggregators(self, sc):
        super(CCIndexWordCountJob, self).log_aggregators(sc)

        self.log_aggregator(sc, self.records_parsing_failed,
                            'records failed to parse = {}')
        self.log_aggregator(sc, self.records_non_html,
                            'records not HTML = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        # sum values of tuple <term_frequency, document_frequency>
        return ((a[0] + b[0]), (a[1] + b[1]))

    def html_to_text(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(page,
                                                               is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text(" ", strip=True)
        except:
            self.records_parsing_failed.add(1)
            return ""

    def process_record(self, record):
        page = record.content_stream().read()
        if not self.is_html(record):
            self.records_non_html.add(1)
            return
        text = self.html_to_text(page, record)
        words = map(lambda w: w.lower(),
                    WordCountJob.word_pattern.findall(text))
        for word, count in Counter(words).items():
            yield word, (count, 1)


if __name__ == '__main__':
    job = CCIndexWordCountJob()
    job.run()
