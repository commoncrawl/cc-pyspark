from fastwarc.warc import ArchiveIterator as FastWarcArchiveIterator
from fastwarc.warc import WarcRecordType, WarcRecord
from fastwarc.stream_io import FastWARCError

from sparkcc import CCSparkJob


class CCFastWarcSparkJob(CCSparkJob):
    """
    Process Common Crawl WARC/WAT/WET files using Spark and FastWARC
    """

    # allow subclasses to filter by record type
    fastwarc_record_filter = WarcRecordType.any_type

    def process_warc(self, uri, stream):
        """Parse a WARC (or WAT/WET file) via FastWARC"""
        try:
            rec_iter = FastWarcArchiveIterator(stream,
                                               record_types=self.fastwarc_record_filter,
                                               parse_http=self.warc_parse_http_header)
            for res in self.iterate_records(uri, rec_iter):
                yield res
        except FastWARCError as exception:
            self.warc_input_failed.add(1)
            self.get_logger().error(
                'Invalid WARC: {} - {}'.format(uri, exception))

    def iterate_records(self, _warc_uri, archive_iterator: FastWarcArchiveIterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res
            self.records_processed.add(1)
            # TODO: how to access WARC record offset and length,
            # cf. fastwarc/cli.py index

    @staticmethod
    def get_payload_stream(record: WarcRecord):
        return record.reader

    @staticmethod
    def get_warc_header(record: WarcRecord, header: str, default: str=None):
        return record.headers.get(header, default)

    @staticmethod
    def get_http_headers(record: WarcRecord):
        return record.http_headers.astuples()

    @staticmethod
    def is_response_record(record: WarcRecord):
        """Return true if WARC record is a WARC response record"""
        return record.record_type == WarcRecordType.response

    @staticmethod
    def is_wet_text_record(record: WarcRecord):
        """Return true if WARC record is a WET text/plain record"""
        return (record.record_type == WarcRecordType.conversion and
                record.headers.get('Content-Type') == 'text/plain')

    @staticmethod
    def is_wat_json_record(record: WarcRecord):
        """Return true if WARC record is a WAT record"""
        return (record.record_type == WarcRecordType.metadata and
                record.headers.get('Content-Type') == 'application/json')

    @staticmethod
    def is_html(record: WarcRecord):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.headers) and
            (record.headers['WARC-Identified-Payload-Type'] in html_types)):
            return True
        for (name, value) in record.http_headers.astuples():
            if name.lower() == 'content-type':
                content_type = value.lower()
                for html_type in html_types:
                    if html_type in content_type:
                        return True
        return False


