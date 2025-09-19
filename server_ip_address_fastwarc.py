from fastwarc.warc import WarcRecordType

from sparkcc_fastwarc import CCFastWarcSparkJob
from server_ip_address import ServerIPAddressJob


class ServerIPAddressFastWarcJob(ServerIPAddressJob, CCFastWarcSparkJob):
    """ Collect server IP addresses from WARC response records
        (WARC and WAT is allowed as input) using FastWARC
        to parse WARC files"""

    name = "ServerIPAddresses"

    # process only WARC request or metadata (including WAT) records
    # Note: restrict the filter accordingly, depending on whether
    #       WARC or WAT files are used
    fastwarc_record_filter = WarcRecordType.request | WarcRecordType.metadata


if __name__ == "__main__":
    job = ServerIPAddressFastWarcJob()
    job.run()
