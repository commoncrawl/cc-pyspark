import ujson as json

try:
    # Python2
    from urlparse import urljoin, urlparse
except ImportError:
    # Python3
    from urllib.parse import urljoin, urlparse

from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob


class ServerIPAddressJob(CCSparkJob):
    """ Gather server IP addresses from WARC response records
        (WARC and WAT is allowed as input)"""

    name = "ServerIPAddresses"

    output_schema = StructType([
        StructField("key", StructType([
            StructField("host", StringType(), True),
            StructField("ip", StringType(), True)]), True),
        StructField("cnt", LongType(), True)
    ])

    response_no_ip_address = '(no IP address)'
    response_no_host = '(no host name)'

    def process_record(self, record):
        ip_address = None
        url = None

        if self.is_wat_json_record(record):
            # WAT (response) record
            record = json.loads(record.content_stream().read())
            try:
                warc_header = record['Envelope']['WARC-Header-Metadata']
                if warc_header['WARC-Type'] != 'response':
                    # WAT request or metadata records
                    return
                if 'WARC-IP-Address' in warc_header:
                    ip_address = warc_header['WARC-IP-Address']
                    url = warc_header['WARC-Target-URI']
                else:
                    # WAT metadata records
                    return
            except KeyError:
                pass
        elif record.rec_type == 'response':
            # WARC response record
            ip_address = record.rec_headers.get_header('WARC-IP-Address')
            url = record.rec_headers.get_header('WARC-Target-URI')
        else:
            # warcinfo, request, non-WAT metadata records
            return

        if not ip_address or ip_address == '':
            ip_address = ServerIPAddressJob.response_no_ip_address

        host_name = ServerIPAddressJob.response_no_host
        if url:
            try:
                host_name = urlparse(url).hostname
            except:
                pass

        yield (host_name, ip_address), 1


if __name__ == "__main__":
    job = ServerIPAddressJob()
    job.run()
