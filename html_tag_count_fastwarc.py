from fastwarc.warc import WarcRecordType

from sparkcc_fastwarc import CCFastWarcSparkJob
from html_tag_count import TagCountJob


class TagCountFastWarcJob(TagCountJob, CCFastWarcSparkJob):
    """ Count HTML tag names in Common Crawl WARC files
        using FastWARC to read WARC files"""

    name = "TagCount"

    fastwarc_record_filter = WarcRecordType.response


if __name__ == '__main__':
    job = TagCountFastWarcJob()
    job.run()
