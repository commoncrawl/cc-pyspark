import re

from collections import Counter

from sparkcc import CCSparkJob


class TagCountJob(CCSparkJob):
    """ Count HTML tag names in Common Crawl WARC files"""

    name = "TagCount"

    # match HTML tags (element names) on binary HTML data
    html_tag_pattern = re.compile(b'<([a-z0-9]+)')

    def process_record(self, record):
        if record.rec_type != 'response':
            # WARC request or metadata records
            return
        content_type = record.http_headers.get_header('content-type', None)
        if content_type is None or 'html' not in content_type:
            # skip non-HTML or unknown content types
            return
        data = record.content_stream().read()
        counts = Counter(TagCountJob.html_tag_pattern.findall(data))
        for tag, count in counts.items():
            yield tag.decode('ascii').lower(), count


if __name__ == '__main__':
    job = TagCountJob()
    job.run()
