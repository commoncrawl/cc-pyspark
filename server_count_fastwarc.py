import ujson as json

from fastwarc.warc import WarcRecordType

from sparkcc_fastwarc import CCFastWarcSparkJob
from server_count import ServerCountJob


class ServerCountFastWarcJob(ServerCountJob, CCFastWarcSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input) using FastWARC
        to parse WARC files"""

    name = "CountServers"

    # process only WARC response and metadata (including WAT) records
    fastwarc_record_filter = WarcRecordType.metadata | WarcRecordType.response

    # process_record is implemented by ServerCountJob


if __name__ == "__main__":
    job = ServerCountFastWarcJob()
    job.run()
