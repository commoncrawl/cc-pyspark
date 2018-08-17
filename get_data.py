from warcio.archiveiterator import ArchiveIterator

urls = []
content = []
with open('/Users/robertdowd/Galvanize/cc-pyspark/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz', 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            urls.append(record.rec_headers.get_header('WARC-Target-URI'))
            #record.content_stream().read())
