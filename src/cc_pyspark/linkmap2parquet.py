from sparkcc import CCSparkJob
from pyspark.sql.types import StructType, StructField, StringType


class LinkMapImportJob(CCSparkJob):
    """Import a map of link pairs <from, to>
    to SparkSQL and save as Parquet"""
    name = "LinkMapImport"

    output_schema = StructType([
        StructField("s", StringType(), True),
        StructField("t", StringType(), True)
    ])

    def map_line(self, line):
        return line.split('\t')

    def run_job(self, session):
        output = None
        if self.args.input != '':
            input_data = session.sparkContext.textFile(
                self.args.input,
                minPartitions=self.args.num_input_partitions)
            output = input_data.map(self.map_line)

        df = session.createDataFrame(output, schema=self.output_schema)

        df.dropDuplicates() \
          .coalesce(self.args.num_output_partitions) \
          .sortWithinPartitions('s', 't') \
          .write \
          .format(self.args.output_format) \
          .option("compression", self.args.output_compression) \
          .saveAsTable(self.args.output)


if __name__ == "__main__":
    job = LinkMapImportJob()
    job.run()
