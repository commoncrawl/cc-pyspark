from sparkcc_fastwarc import CCFastWarcSparkJob
from word_count import WordCountJob

class WordCountFastWarcJob(WordCountJob, CCFastWarcSparkJob):
    """ Word count (frequency list) from texts in Common Crawl WET files
        using FastWARC to read WET files"""

    name = "WordCount"


if __name__ == '__main__':
    job = WordCountFastWarcJob()
    job.run()
