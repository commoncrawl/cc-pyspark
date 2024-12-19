import subprocess
import os
from sparkcc import CCFileProcessorSparkJob
from pyspark.sql.types import StructType, StructField, StringType

class MD5Sum(CCFileProcessorSparkJob):
    """ MD5 sum of each file"""

    name = "MD5Sum"

    output_schema = StructType([
        StructField("uri", StringType(), True),
        StructField("md5", StringType(), True),
    ])

    def process_file(self, uri, tempfd):
        proc = subprocess.run(['md5sum', tempfd.name], capture_output=True, check=True, encoding='utf8')
        digest = proc.stdout.rstrip().split()[0]
        yield uri, digest

if __name__ == '__main__':
    job = MD5Sum()
    job.run()
